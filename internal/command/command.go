package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"

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
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n", args[0])
		writeUsage(stderr)
		return exitUsage
	}
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
  server    Run the HTTP server
  version   Print build version information
  help      Show this help

Run "open-aspm server --help" for server options.`)
}
