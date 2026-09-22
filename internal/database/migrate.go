// Package database owns PostgreSQL connection and schema migration infrastructure.
package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Status describes the installed and available schema versions.
type Status struct {
	Current int64
	Target  int64
	Pending bool
}

// Migrator applies the migrations embedded in the open-aspm binary.
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

// Open connects to PostgreSQL and prepares a migrator. The URL is never logged.
func Open(ctx context.Context, databaseURL string) (*Migrator, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("database URL is empty")
	}

	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL connection: %w", err)
	}
	// A session-level advisory lock occupies one connection while migrations run.
	db.SetMaxOpenConns(4)

	migrator, err := newMigrator(db, embeddedMigrations)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := migrator.provider.Ping(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	return migrator, nil
}

func newMigrator(db *sql.DB, migrationFS fs.FS) (*Migrator, error) {
	migrations, err := fs.Sub(migrationFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return nil, fmt.Errorf("configure migration lock: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		migrations,
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		return nil, fmt.Errorf("load migrations: %w", err)
	}
	return &Migrator{db: db, provider: provider}, nil
}

// Up applies every pending migration. Each migration is transactional unless
// its SQL file explicitly opts out.
func (m *Migrator) Up(ctx context.Context) (Status, int, error) {
	results, err := m.provider.Up(ctx)
	if err != nil {
		return Status{}, 0, fmt.Errorf("apply migrations: %w", err)
	}
	status, err := m.Status(ctx)
	if err != nil {
		return Status{}, len(results), err
	}
	return status, len(results), nil
}

// Status reports the database version and whether migrations remain pending.
func (m *Migrator) Status(ctx context.Context) (Status, error) {
	current, target, err := m.provider.GetVersions(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("read migration versions: %w", err)
	}
	pending, err := m.provider.HasPending(ctx)
	if err != nil {
		return Status{}, fmt.Errorf("read migration status: %w", err)
	}
	return Status{Current: current, Target: target, Pending: pending}, nil
}

// Close releases database resources.
func (m *Migrator) Close() error {
	return m.db.Close()
}
