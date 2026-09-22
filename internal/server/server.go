package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"
)

const (
	defaultListenAddress  = "127.0.0.1:8080"
	defaultShutdownPeriod = 10 * time.Second
)

// Config contains HTTP server settings that are safe to expose through the
// command line.
type Config struct {
	ListenAddress   string
	ShutdownTimeout time.Duration
}

// DefaultConfig returns conservative defaults for local development. A
// deployment must opt in to listening on a non-loopback interface.
func DefaultConfig() Config {
	return Config{
		ListenAddress:   defaultListenAddress,
		ShutdownTimeout: defaultShutdownPeriod,
	}
}

// Run opens the configured listener and serves requests until the context is
// cancelled or the server fails.
func Run(ctx context.Context, config Config, logger *slog.Logger) error {
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}

	logger.Info("server listening", "address", listener.Addr().String())
	return Serve(ctx, listener, NewHandler(), logger, config.ShutdownTimeout)
}

// Serve runs the HTTP server on an existing listener. Accepting a listener
// makes lifecycle behavior testable without reserving a fixed port.
func Serve(
	ctx context.Context,
	listener net.Listener,
	handler http.Handler,
	logger *slog.Logger,
	shutdownTimeout time.Duration,
) error {
	httpServer := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}

	shutdownResult := make(chan error, 1)
	serveFinished := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
			defer cancel()

			shutdownErr := httpServer.Shutdown(shutdownContext)
			if shutdownErr != nil {
				shutdownErr = errors.Join(shutdownErr, httpServer.Close())
			}
			shutdownResult <- shutdownErr
		case <-serveFinished:
			shutdownResult <- nil
		}
	}()

	serveErr := httpServer.Serve(listener)
	close(serveFinished)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}

	shutdownErr := <-shutdownResult
	if shutdownErr != nil {
		return fmt.Errorf("gracefully shut down HTTP server: %w", shutdownErr)
	}
	if serveErr != nil {
		return fmt.Errorf("serve HTTP: %w", serveErr)
	}

	logger.Info("server stopped")
	return nil
}

// NewHandler returns the initial HTTP routing surface. Health endpoints reveal
// no build, host, or dependency details.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", health)
	mux.HandleFunc("GET /health/ready", health)

	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Cache-Control", "no-store")
		response.Header().Set("X-Content-Type-Options", "nosniff")
		mux.ServeHTTP(response, request)
	})
}

func health(response http.ResponseWriter, _ *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(response, "{\"status\":\"ok\"}\n")
}
