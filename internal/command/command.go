package command

import (
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/spectremi/open-aspm/internal/authentication"
	"github.com/spectremi/open-aspm/internal/bootstrap"
	"github.com/spectremi/open-aspm/internal/database"
	"github.com/spectremi/open-aspm/internal/server"
	"github.com/spectremi/open-aspm/internal/version"
)

const (
	exitOK      = 0
	exitFailure = 1
	exitUsage   = 2
)

// Run executes the command and returns a process exit code.
func Run(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	logger *slog.Logger,
) int {
	if len(args) == 0 {
		writeUsage(stdout)
		return exitOK
	}

	switch args[0] {
	case "help", "-h", "--help":
		writeUsage(stdout)
		return exitOK
	case "version":
		_, _ = fmt.Fprintln(stdout, version.Current())
		return exitOK
	case "server":
		return runServer(ctx, args[1:], stderr, logger)
	case "migrate":
		return runMigrate(ctx, args[1:], stdout, stderr, logger)
	case "bootstrap":
		return runBootstrap(ctx, args[1:], stdout, stderr, logger)
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		writeUsage(stderr)
		return exitUsage
	}
}

func runBootstrap(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	logger *slog.Logger,
) int {
	if len(args) != 1 || (args[0] != "init" && args[0] != "token") {
		_, _ = fmt.Fprintln(stderr, "Usage: open-aspm bootstrap <init|token>")
		return exitUsage
	}
	databaseURL := os.Getenv("OPEN_ASPM_DATABASE_URL")
	if databaseURL == "" {
		_, _ = fmt.Fprintln(stderr, "OPEN_ASPM_DATABASE_URL is required")
		return exitUsage
	}
	authenticationConfig, ok := readAuthenticationConfig(stderr)
	if !ok {
		return exitUsage
	}
	db, err := database.Connect(ctx, databaseURL)
	if err != nil {
		logger.Error("bootstrap database connection failed", "error", err)
		return exitFailure
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("bootstrap database close failed", "error", err)
		}
	}()
	authenticationStore, err := authentication.NewPostgresStore(db)
	if err != nil {
		logger.Error("bootstrap authentication setup failed", "error", err)
		return exitFailure
	}
	tokenService, err := authentication.NewService(authenticationStore, authenticationConfig)
	if err != nil {
		logger.Error("bootstrap token configuration failed", "error", err)
		return exitFailure
	}
	bootstrapStore, err := bootstrap.NewPostgresStore(db)
	if err != nil {
		logger.Error("bootstrap storage setup failed", "error", err)
		return exitFailure
	}
	service, err := bootstrap.NewService(
		bootstrapStore, tokenService, bootstrap.Config{TokenTTL: 30 * 24 * time.Hour},
	)
	if err != nil {
		logger.Error("bootstrap service setup failed", "error", err)
		return exitFailure
	}
	var result bootstrap.Result
	if args[0] == "init" {
		result, err = service.Initialize(ctx)
	} else {
		result, err = service.RotateToken(ctx)
	}
	if err != nil {
		logger.Error("operator bootstrap failed", "error", err)
		return exitFailure
	}
	_, _ = fmt.Fprintf(stdout,
		"workspace_id=%s\napplication_id=%s\nprincipal_id=%s\ntoken_id=%s\napi_token=%s\ntoken_expires_at=%s\n",
		result.WorkspaceID, result.ApplicationID, result.PrincipalID, result.TokenID,
		result.Token, result.TokenExpires.Format(time.RFC3339),
	)
	_, _ = fmt.Fprintln(stderr, "Store api_token securely now; it cannot be retrieved later.")
	return exitOK
}

func runMigrate(
	ctx context.Context,
	args []string,
	stdout io.Writer,
	stderr io.Writer,
	logger *slog.Logger,
) int {
	if len(args) != 1 || (args[0] != "up" && args[0] != "status") {
		_, _ = fmt.Fprintln(stderr, "Usage: open-aspm migrate <up|status>")
		return exitUsage
	}

	databaseURL := os.Getenv("OPEN_ASPM_DATABASE_URL")
	if databaseURL == "" {
		_, _ = fmt.Fprintln(stderr, "OPEN_ASPM_DATABASE_URL is required")
		return exitUsage
	}
	migrator, err := database.Open(ctx, databaseURL)
	if err != nil {
		logger.Error("database migration failed", "error", err)
		return exitFailure
	}
	defer func() {
		if err := migrator.Close(); err != nil {
			logger.Warn("database close failed", "error", err)
		}
	}()

	if args[0] == "status" {
		status, err := migrator.Status(ctx)
		if err != nil {
			logger.Error("database migration status failed", "error", err)
			return exitFailure
		}
		_, _ = fmt.Fprintf(
			stdout,
			"database version: %d; target: %d; pending: %t\n",
			status.Current,
			status.Target,
			status.Pending,
		)
		return exitOK
	}

	status, applied, err := migrator.Up(ctx)
	if err != nil {
		logger.Error("database migration failed", "error", err)
		return exitFailure
	}
	_, _ = fmt.Fprintf(stdout, "database migrated: version %d; applied: %d\n", status.Current, applied)
	return exitOK
}

func runServer(ctx context.Context, args []string, stderr io.Writer, logger *slog.Logger) int {
	config := server.DefaultConfig()
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&config.ListenAddress, "listen", config.ListenAddress, "HTTP listen address")
	flags.DurationVar(
		&config.ShutdownTimeout,
		"shutdown-timeout",
		config.ShutdownTimeout,
		"graceful shutdown timeout",
	)
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: open-aspm server [options]")
		_, _ = fmt.Fprintln(stderr)
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "unexpected server argument %q\n", flags.Arg(0))
		return exitUsage
	}
	if config.ShutdownTimeout <= 0 {
		_, _ = fmt.Fprintln(stderr, "shutdown-timeout must be greater than zero")
		return exitUsage
	}
	databaseURL := os.Getenv("OPEN_ASPM_DATABASE_URL")
	if databaseURL == "" {
		_, _ = fmt.Fprintln(stderr, "OPEN_ASPM_DATABASE_URL is required")
		return exitUsage
	}
	authenticationConfig, ok := readAuthenticationConfig(stderr)
	if !ok {
		return exitUsage
	}
	db, err := database.Connect(ctx, databaseURL)
	if err != nil {
		logger.Error("server database connection failed", "error", err)
		return exitFailure
	}
	defer func() {
		if err := db.Close(); err != nil {
			logger.Warn("server database close failed", "error", err)
		}
	}()
	handler, err := server.NewRuntimeHandler(db, authenticationConfig, logger)
	if err != nil {
		logger.Error("server runtime setup failed", "error", err)
		return exitFailure
	}

	if err := server.Run(ctx, config, handler, logger); err != nil {
		logger.Error("server failed", "error", err)
		return exitFailure
	}

	return exitOK
}

func readAuthenticationConfig(stderr io.Writer) (authentication.Config, bool) {
	keyID := os.Getenv("OPEN_ASPM_TOKEN_VERIFIER_KEY_ID")
	if keyID == "" {
		keyID = "bootstrap-v1"
	}
	encodedKey := os.Getenv("OPEN_ASPM_TOKEN_VERIFIER_KEY")
	key, err := base64.RawURLEncoding.DecodeString(encodedKey)
	if err != nil || len(key) < 32 {
		_, _ = fmt.Fprintln(stderr, "OPEN_ASPM_TOKEN_VERIFIER_KEY must be unpadded base64url encoding of at least 32 bytes")
		return authentication.Config{}, false
	}
	return authentication.Config{
		ActiveKeyID: keyID,
		Keys:        map[string][]byte{keyID: key},
	}, true
}

func writeUsage(output io.Writer) {
	_, _ = fmt.Fprintln(output, `Open ASPM

Usage:
  open-aspm <command> [options]

Commands:
	bootstrap Initialize the first workspace or rotate its operator token
	migrate   Inspect or apply PostgreSQL schema migrations
  server    Run the HTTP server
  version   Print build version information
  help      Show this help

Run "open-aspm server --help" for server options.
Set OPEN_ASPM_DATABASE_URL before running database-backed commands.
The server and bootstrap commands also require OPEN_ASPM_TOKEN_VERIFIER_KEY.`)
}
