//go:build integration

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spectremi/open-aspm/internal/authentication"
	"github.com/spectremi/open-aspm/internal/bootstrap"
	"github.com/spectremi/open-aspm/internal/database"
)

func TestPostgresAuthenticatedRepositoryCreationHTTP(t *testing.T) {
	ctx, db := openServerTestDatabase(t)
	key := []byte("integration-test-server-key-32-bytes-minimum")
	authenticationConfig := authentication.Config{
		ActiveKeyID: "test-key",
		Keys:        map[string][]byte{"test-key": key},
	}
	authenticationStore, err := authentication.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}
	tokenService, err := authentication.NewService(authenticationStore, authenticationConfig)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapStore, err := bootstrap.NewPostgresStore(db)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapService, err := bootstrap.NewService(
		bootstrapStore,
		tokenService,
		bootstrap.Config{TokenTTL: 30 * 24 * time.Hour},
	)
	if err != nil {
		t.Fatal(err)
	}
	installation, err := bootstrapService.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	handler, err := NewRuntimeHandler(
		db,
		authenticationConfig,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	if err != nil {
		t.Fatalf("NewRuntimeHandler() error = %v", err)
	}

	request := func(displayName string) *http.Request {
		httpRequest := httptest.NewRequest(
			http.MethodPost,
			"/api/v1/workspaces/"+installation.WorkspaceID+"/repositories",
			strings.NewReader(`{"display_name":`+strconv.Quote(displayName)+`}`),
		)
		httpRequest.Header.Set("Authorization", "Bearer "+installation.Token)
		httpRequest.Header.Set("Content-Type", "application/json")
		httpRequest.Header.Set("Idempotency-Key", "integration-request-1")
		return httpRequest
	}

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request("example-service"))
	if first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, body = %s", first.Code, first.Body.String())
	}
	var created repositoryResponse
	if err := json.NewDecoder(first.Body).Decode(&created); err != nil {
		t.Fatal(err)
	}
	if created.ID == "" || created.WorkspaceID != installation.WorkspaceID ||
		created.CreatedByPrincipalID != installation.PrincipalID || created.DisplayName != "example-service" {
		t.Fatalf("created repository = %+v", created)
	}

	replayed := httptest.NewRecorder()
	handler.ServeHTTP(replayed, request("example-service"))
	if replayed.Code != http.StatusCreated {
		t.Fatalf("replay status = %d, body = %s", replayed.Code, replayed.Body.String())
	}
	var replayedRepository repositoryResponse
	if err := json.NewDecoder(replayed.Body).Decode(&replayedRepository); err != nil {
		t.Fatal(err)
	}
	if replayedRepository.ID != created.ID {
		t.Fatalf("replay repository ID = %q, want %q", replayedRepository.ID, created.ID)
	}

	conflict := httptest.NewRecorder()
	handler.ServeHTTP(conflict, request("different-service"))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d, body = %s", conflict.Code, conflict.Body.String())
	}

	if _, err := db.ExecContext(ctx, `
		DELETE FROM open_aspm.api_token_capabilities
		WHERE workspace_id = $1 AND token_id = $2
		  AND capability = 'catalog:repositories:create'`,
		installation.WorkspaceID, installation.TokenID,
	); err != nil {
		t.Fatalf("remove token capability: %v", err)
	}
	deniedRequest := request("not-created")
	deniedRequest.Header.Set("Idempotency-Key", "integration-request-2")
	denied := httptest.NewRecorder()
	handler.ServeHTTP(denied, deniedRequest)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("token-scope denial status = %d, body = %s", denied.Code, denied.Body.String())
	}

	var repositoryCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.repositories WHERE workspace_id = $1`,
		installation.WorkspaceID,
	).Scan(&repositoryCount); err != nil {
		t.Fatal(err)
	}
	if repositoryCount != 1 {
		t.Fatalf("repository count = %d, want 1", repositoryCount)
	}
}

func openServerTestDatabase(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	adminURL := os.Getenv("OPEN_ASPM_TEST_DATABASE_ADMIN_URL")
	if adminURL == "" {
		t.Skip("OPEN_ASPM_TEST_DATABASE_ADMIN_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	admin, err := sql.Open("pgx", adminURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	databaseName := "open_aspm_server_" + suffix
	ownerRole := "open_aspm_server_owner_" + suffix
	password := "integration-test-only"
	databaseIdentifier := pgx.Identifier{databaseName}.Sanitize()
	ownerIdentifier := pgx.Identifier{ownerRole}.Sanitize()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
		ownerIdentifier,
	)); err != nil {
		t.Fatalf("create test role: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE %s OWNER %s", databaseIdentifier, ownerIdentifier,
	)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf(
			"DROP DATABASE IF EXISTS %s WITH (FORCE)", databaseIdentifier,
		))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", ownerIdentifier))
	})

	databaseURL := serverTestDatabaseURL(t, adminURL, databaseName, ownerRole, password)
	migrator, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := migrator.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := migrator.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := database.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return ctx, db
}

func serverTestDatabaseURL(t *testing.T, baseURL, databaseName, user, password string) string {
	t.Helper()
	config, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.Path = "/" + databaseName
	config.User = url.UserPassword(user, password)
	return config.String()
}
