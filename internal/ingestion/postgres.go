package ingestion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// PostgresStore persists ingestion-owned reservation state.
type PostgresStore struct {
	db *sql.DB
}

// NewPostgresStore returns an ingestion store backed by an existing pool.
func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	if db == nil {
		return nil, ErrInvalid
	}
	return &PostgresStore{db: db}, nil
}

// Reserve creates one import and its completed idempotency entry atomically.
// The deferred foreign key lets the idempotency claim win before the import is
// inserted, so concurrent retries never create committed duplicate imports.
func (store *PostgresStore) Reserve(ctx context.Context, spec reserveSpec) (Reservation, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Reservation{}, fmt.Errorf("begin import reservation: %w", err)
	}
	defer tx.Rollback()
	var lockAcquired bool
	if err := tx.QueryRowContext(ctx,
		`SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`,
		"open-aspm-import-idempotency:"+idempotencyLockKey(spec),
	).Scan(&lockAcquired); err != nil {
		return Reservation{}, fmt.Errorf("lock import idempotency scope: %w", err)
	}
	if !lockAcquired {
		return Reservation{}, ErrIdempotencyInProgress
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.import_create_idempotency (
			workspace_id, principal_id, api_major_version, operation,
			idempotency_key, request_fingerprint, import_id, completed_at, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (
			workspace_id, principal_id, api_major_version, operation, idempotency_key
		) DO NOTHING`,
		spec.Import.WorkspaceID, spec.PrincipalID, spec.APIMajorVersion, spec.Operation,
		spec.IdempotencyKey, spec.RequestFingerprint[:], spec.Import.ID,
		spec.Import.CreatedAt, spec.IdempotencyExpiresAt,
	)
	if err != nil {
		return Reservation{}, classifyPostgresError("claim import idempotency key", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Reservation{}, fmt.Errorf("read idempotency claim result: %w", err)
	}
	if rows == 0 {
		reservation, fingerprint, err := selectReservation(
			ctx, tx, spec.Import.WorkspaceID, spec.PrincipalID, spec.APIMajorVersion,
			spec.Operation, spec.IdempotencyKey,
		)
		if err != nil {
			return Reservation{}, err
		}
		if !bytes.Equal(fingerprint, spec.RequestFingerprint[:]) {
			return Reservation{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return Reservation{}, fmt.Errorf("finish import reservation replay: %w", err)
		}
		reservation.Created = false
		return reservation, nil
	}
	if spec.RequestExceedsLimit {
		return Reservation{}, ErrTooLarge
	}

	expectedDigest, err := optionalDigest(spec.Import.ExpectedSHA256)
	if err != nil {
		return Reservation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.imports (
			workspace_id, id, application_id, created_by_principal_id, state,
			report_format_name, report_format_version, original_filename,
			expected_size_bytes, expected_sha256, max_bytes, upload_expires_at,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, NULLIF($7::varchar, ''), NULLIF($8::varchar, ''),
			$9, $10, $11, $12, $13, $14
		)`,
		spec.Import.WorkspaceID, spec.Import.ID, spec.Import.ApplicationID,
		spec.Import.CreatedByPrincipalID, spec.Import.State, spec.Import.ReportFormat.Name,
		spec.Import.ReportFormat.Version, spec.Import.OriginalFilename,
		spec.Import.ExpectedSize, expectedDigest, spec.Import.MaxBytes,
		spec.Import.UploadExpiresAt, spec.Import.CreatedAt, spec.Import.UpdatedAt,
	); err != nil {
		return Reservation{}, classifyPostgresError("insert import reservation", err)
	}
	if err := tx.Commit(); err != nil {
		return Reservation{}, classifyPostgresError("commit import reservation", err)
	}
	return Reservation{Import: spec.Import, Created: true}, nil
}

type rowScanner interface {
	Scan(...any) error
}

func selectReservation(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID, principalID string,
	apiVersion int,
	operation, idempotencyKey string,
) (Reservation, []byte, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT idem.request_fingerprint,
		       imp.id, imp.workspace_id, imp.application_id, imp.created_by_principal_id,
		       imp.state, imp.report_format_name, imp.report_format_version,
		       imp.original_filename, imp.expected_size_bytes, imp.expected_sha256,
		       imp.max_bytes, imp.upload_expires_at, imp.created_at, imp.updated_at
		FROM open_aspm.import_create_idempotency AS idem
		JOIN open_aspm.imports AS imp
		  ON imp.workspace_id = idem.workspace_id AND imp.id = idem.import_id
		WHERE idem.workspace_id = $1 AND idem.principal_id = $2
		  AND idem.api_major_version = $3 AND idem.operation = $4
		  AND idem.idempotency_key = $5`,
		workspaceID, principalID, apiVersion, operation, idempotencyKey,
	)
	var fingerprint []byte
	importRecord, err := scanImport(row, &fingerprint)
	if err != nil {
		return Reservation{}, nil, fmt.Errorf("read import reservation replay: %w", err)
	}
	return Reservation{Import: importRecord}, fingerprint, nil
}

func scanImport(row rowScanner, fingerprint *[]byte) (Import, error) {
	var record Import
	var reportVersion, originalFilename sql.NullString
	var expectedSize sql.NullInt64
	var expectedDigest []byte
	if err := row.Scan(
		fingerprint,
		&record.ID, &record.WorkspaceID, &record.ApplicationID, &record.CreatedByPrincipalID,
		&record.State, &record.ReportFormat.Name, &reportVersion, &originalFilename,
		&expectedSize, &expectedDigest, &record.MaxBytes, &record.UploadExpiresAt,
		&record.CreatedAt, &record.UpdatedAt,
	); err != nil {
		return Import{}, err
	}
	record.ReportFormat.Version = reportVersion.String
	record.OriginalFilename = originalFilename.String
	if expectedSize.Valid {
		value := expectedSize.Int64
		record.ExpectedSize = &value
	}
	if len(expectedDigest) != 0 {
		record.ExpectedSHA256 = hex.EncodeToString(expectedDigest)
	}
	return record, nil
}

func optionalDigest(value string) ([]byte, error) {
	if value == "" {
		return nil, nil
	}
	digest, err := hex.DecodeString(value)
	if err != nil || len(digest) != 32 {
		return nil, ErrInvalid
	}
	return digest, nil
}

func classifyPostgresError(action string, err error) error {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.ConstraintName == "imports_application_fk" {
		return ErrApplicationNotFound
	}
	return fmt.Errorf("%s: %w", action, err)
}
