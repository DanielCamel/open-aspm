package command

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestRunHelp(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(
		context.Background(),
		nil,
		&stdout,
		&stderr,
		slog.New(slog.NewJSONHandler(&stderr, nil)),
	)

	if exitCode != exitOK {
		t.Fatalf("Run() exit code = %d, want %d", exitCode, exitOK)
	}
	if !strings.Contains(stdout.String(), "Usage:") {
		t.Fatalf("Run() stdout = %q, want usage", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("Run() stderr = %q, want empty", stderr.String())
	}
}

func TestRunVersion(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(
		context.Background(),
		[]string{"version"},
		&stdout,
		&stderr,
		slog.New(slog.NewJSONHandler(&stderr, nil)),
	)

	if exitCode != exitOK {
		t.Fatalf("Run() exit code = %d, want %d", exitCode, exitOK)
	}
	if !strings.HasPrefix(stdout.String(), "open-aspm ") {
		t.Fatalf("Run() stdout = %q, want version", stdout.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(
		context.Background(),
		[]string{"unknown"},
		&stdout,
		&stderr,
		slog.New(slog.NewJSONHandler(&stderr, nil)),
	)

	if exitCode != exitUsage {
		t.Fatalf("Run() exit code = %d, want %d", exitCode, exitUsage)
	}
	if !strings.Contains(stderr.String(), `unknown command "unknown"`) {
		t.Fatalf("Run() stderr = %q, want unknown-command error", stderr.String())
	}
}

func TestRunServerRejectsInvalidShutdownTimeout(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(
		context.Background(),
		[]string{"server", "--shutdown-timeout=0s"},
		&stdout,
		&stderr,
		slog.New(slog.NewJSONHandler(&stderr, nil)),
	)

	if exitCode != exitUsage {
		t.Fatalf("Run() exit code = %d, want %d", exitCode, exitUsage)
	}
	if !strings.Contains(stderr.String(), "must be greater than zero") {
		t.Fatalf("Run() stderr = %q, want validation error", stderr.String())
	}
}

func TestRunServerLogsStartupFailureAsJSON(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	exitCode := Run(
		context.Background(),
		[]string{"server", "--listen=not-an-address"},
		&stdout,
		&stderr,
		slog.New(slog.NewJSONHandler(&stderr, nil)),
	)

	if exitCode != exitFailure {
		t.Fatalf("Run() exit code = %d, want %d", exitCode, exitFailure)
	}
	if !strings.Contains(stderr.String(), `"msg":"server failed"`) {
		t.Fatalf("Run() stderr = %q, want structured startup error", stderr.String())
	}
}
