//go:build integration

package database

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strconv"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestMigrationsRunAsNonSuperuser(t *testing.T) {
	adminURL := os.Getenv("OPEN_ASPM_TEST_DATABASE_ADMIN_URL")
	if adminURL == "" {
		t.Skip("OPEN_ASPM_TEST_DATABASE_ADMIN_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := sql.Open("pgx", adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	databaseName := "open_aspm_test_" + suffix
	migrationRole := "open_aspm_migration_" + suffix
	runtimeRole := "open_aspm_runtime_" + suffix
	password := "integration-test-only"

	createRole(t, ctx, admin, migrationRole, password)
	createRole(t, ctx, admin, runtimeRole, password)
	databaseIdentifier := pgx.Identifier{databaseName}.Sanitize()
	migrationIdentifier := pgx.Identifier{migrationRole}.Sanitize()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE %s OWNER %s", databaseIdentifier, migrationIdentifier,
	)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", databaseIdentifier))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{runtimeRole}.Sanitize()))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", migrationIdentifier))
	})

	assertNotSuperuser(t, ctx, admin, migrationRole)
	assertNotSuperuser(t, ctx, admin, runtimeRole)

	migrationURL := databaseURL(t, adminURL, databaseName, migrationRole, password)
	migrator, err := Open(ctx, migrationURL)
	if err != nil {
		t.Fatalf("open migrator: %v", err)
	}
	status, applied, err := migrator.Up(ctx)
	if err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if applied != 4 || status.Current != 4 || status.Target != 4 || status.Pending {
		t.Fatalf("first Up() = (%+v, %d), want version 4 with four applied migrations", status, applied)
	}
	status, applied, err = migrator.Up(ctx)
	if err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
	if applied != 0 || status.Pending {
		t.Fatalf("second Up() = (%+v, %d), want no-op", status, applied)
	}
	if err := migrator.Close(); err != nil {
		t.Fatalf("close migrator: %v", err)
	}

	grantRuntimeAccess(t, ctx, adminURL, databaseName, migrationRole, password, runtimeRole)
	runtimeDB, err := sql.Open("pgx", databaseURL(t, adminURL, databaseName, runtimeRole, password))
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeDB.Close()
	if _, err := runtimeDB.ExecContext(ctx, "CREATE TABLE open_aspm.must_be_denied (id integer)"); err == nil {
		t.Fatal("runtime role unexpectedly created a table")
	}

	assertFailedMigrationDoesNotAdvanceVersion(t, ctx, migrationURL)
}

func createRole(t *testing.T, ctx context.Context, admin *sql.DB, name, password string) {
	t.Helper()
	if password != "integration-test-only" {
		t.Fatal("integration test role password changed unexpectedly")
	}
	query := fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
		pgx.Identifier{name}.Sanitize(),
	)
	if _, err := admin.ExecContext(ctx, query); err != nil {
		t.Fatalf("create role %s: %v", name, err)
	}
}

func assertNotSuperuser(t *testing.T, ctx context.Context, admin *sql.DB, role string) {
	t.Helper()
	var superuser bool
	if err := admin.QueryRowContext(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = $1", role).Scan(&superuser); err != nil {
		t.Fatalf("read role %s: %v", role, err)
	}
	if superuser {
		t.Fatalf("role %s must not be a superuser", role)
	}
}

func databaseURL(t *testing.T, baseURL, databaseName, user, password string) string {
	t.Helper()
	config, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.Path = "/" + databaseName
	config.User = url.UserPassword(user, password)
	return config.String()
}

func grantRuntimeAccess(
	t *testing.T,
	ctx context.Context,
	adminURL, databaseName, migrationRole, password, runtimeRole string,
) {
	t.Helper()
	db, err := sql.Open("pgx", databaseURL(t, adminURL, databaseName, migrationRole, password))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		"GRANT USAGE ON SCHEMA open_aspm TO %s", pgx.Identifier{runtimeRole}.Sanitize(),
	)); err != nil {
		t.Fatalf("grant runtime access: %v", err)
	}
}

func assertFailedMigrationDoesNotAdvanceVersion(t *testing.T, ctx context.Context, databaseURL string) {
	t.Helper()
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	brokenFS := fstest.MapFS{
		"migrations/00001_initial.sql": &fstest.MapFile{Data: []byte("-- +goose Up\nSELECT 1;\n")},
		"migrations/00002_second.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\nSELECT 1;\n")},
		"migrations/00003_third.sql":   &fstest.MapFile{Data: []byte("-- +goose Up\nSELECT 1;\n")},
		"migrations/00004_fourth.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\nSELECT 1;\n")},
		"migrations/00005_broken.sql":  &fstest.MapFile{Data: []byte("-- +goose Up\nSELECT * FROM open_aspm.table_that_does_not_exist;\n")},
	}
	migrator, err := newMigrator(db, fs.FS(brokenFS))
	if err != nil {
		t.Fatalf("create broken migrator: %v", err)
	}
	if _, _, err := migrator.Up(ctx); err == nil {
		t.Fatal("broken migration unexpectedly succeeded")
	}
	status, err := migrator.Status(ctx)
	if err != nil {
		t.Fatalf("read status after failed migration: %v", err)
	}
	if status.Current != 4 || !status.Pending {
		t.Fatalf("status after failed migration = %+v, want current 4 and pending", status)
	}
}
