package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigUsesLoopback(t *testing.T) {
	config := DefaultConfig()
	if config.ListenAddress != "127.0.0.1:8080" {
		t.Fatalf("DefaultConfig().ListenAddress = %q, want loopback", config.ListenAddress)
	}
	if config.ShutdownTimeout <= 0 {
		t.Fatalf("DefaultConfig().ShutdownTimeout = %s, want positive duration", config.ShutdownTimeout)
	}
}

func TestHealthEndpoints(t *testing.T) {
	for _, path := range []string{"/health/live", "/health/ready"} {
		t.Run(path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, path, nil)
			response := httptest.NewRecorder()

			NewHandler().ServeHTTP(response, request)

			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
			}
			if response.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type = %q, want application/json", response.Header().Get("Content-Type"))
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", response.Header().Get("Cache-Control"))
			}
			if response.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf(
					"X-Content-Type-Options = %q, want nosniff",
					response.Header().Get("X-Content-Type-Options"),
				)
			}
			if response.Body.String() != "{\"status\":\"ok\"}\n" {
				t.Fatalf("body = %q, want stable health response", response.Body.String())
			}
		})
	}
}

func TestHealthEndpointRejectsPost(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/health/live", strings.NewReader("ignored"))
	response := httptest.NewRecorder()

	NewHandler().ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusMethodNotAllowed)
	}
}

func TestServeShutsDownAfterContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		result <- Serve(ctx, listener, NewHandler(), logger, time.Second)
	}()

	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/health/live")
	if err != nil {
		cancel()
		t.Fatalf("GET health endpoint: %v", err)
	}
	_ = response.Body.Close()

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Serve() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve() did not stop after context cancellation")
	}
}
