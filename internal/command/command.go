package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"

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
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		writeUsage(stderr)
		return exitUsage
	}
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

	if err := server.Run(ctx, config, logger); err != nil {
		logger.Error("server failed", "error", err)
		return exitFailure
	}

	return exitOK
}

func writeUsage(output io.Writer) {
	_, _ = fmt.Fprintln(output, `Open ASPM

Usage:
  open-aspm <command> [options]

Commands:
  migrate   Inspect or apply PostgreSQL schema migrations
  server    Run the HTTP server
  version   Print build version information
  help      Show this help

Run "open-aspm server --help" for server options.
Set OPEN_ASPM_DATABASE_URL before running migration commands.`)
}
