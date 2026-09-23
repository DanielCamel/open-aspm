package ingestion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spectremi/open-aspm/internal/blobstore"
)

// PostgresStore persists ingestion-owned reservation state.
type PostgresStore struct {
	db *sql.DB
}

// BeginUpload fences one streaming attempt and returns immutable reservation
// expectations. The transaction ends before BlobStore I/O begins.
func (store *PostgresStore) BeginUpload(ctx context.Context, spec beginUploadSpec) (uploadSession, error) {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return uploadSession{}, fmt.Errorf("begin raw artifact upload: %w", err)
	}
	defer tx.Rollback()

	var importState ImportState
	var maxBytes int64
	var uploadExpiresAt time.Time
	var expectedSize sql.NullInt64
	var expectedDigest []byte
	var artifactID, artifactState, storageBackend, storageKey, currentAttempt sql.NullString
	var leaseExpiresAt sql.NullTime
	var sizeBytes sql.NullInt64
	var digest []byte
	var storageVersion, backendVersion sql.NullString
	var committedAt sql.NullTime
	err = tx.QueryRowContext(ctx, `
		SELECT imp.state, imp.max_bytes, imp.upload_expires_at,
		       imp.expected_size_bytes, imp.expected_sha256,
		       art.id, art.state, art.storage_backend, art.storage_key, art.upload_attempt_id,
		       art.upload_lease_expires_at, art.size_bytes, art.sha256,
		       art.storage_version, art.backend_version, art.committed_at
		FROM open_aspm.imports AS imp
		LEFT JOIN open_aspm.raw_artifacts AS art
		  ON art.workspace_id = imp.workspace_id AND art.import_id = imp.id
		WHERE imp.workspace_id = $1 AND imp.id = $2
		  AND imp.application_id = $3 AND imp.created_by_principal_id = $4
		FOR UPDATE OF imp`,
		spec.WorkspaceID, spec.ImportID, spec.ApplicationID, spec.PrincipalID,
	).Scan(
		&importState, &maxBytes, &uploadExpiresAt, &expectedSize, &expectedDigest,
		&artifactID, &artifactState, &storageBackend, &storageKey, &currentAttempt, &leaseExpiresAt,
		&sizeBytes, &digest, &storageVersion, &backendVersion, &committedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return uploadSession{}, ErrImportNotFound
	}
	if err != nil {
		return uploadSession{}, fmt.Errorf("read import upload reservation: %w", err)
	}

	session := uploadSession{
		WorkspaceID: spec.WorkspaceID, ImportID: spec.ImportID, MaxBytes: maxBytes,
		ExpectedSHA256: hex.EncodeToString(expectedDigest),
	}
	if expectedSize.Valid {
		value := expectedSize.Int64
		session.ExpectedSize = &value
	}
	if importState == ImportUploaded {
		if !artifactID.Valid || rawArtifactState(artifactState.String) != rawArtifactCommitted ||
			!sizeBytes.Valid || len(digest) != 32 || !committedAt.Valid {
			return uploadSession{}, fmt.Errorf("read committed raw artifact: %w", blobstore.ErrIntegrity)
		}
		session.ArtifactID = artifactID.String
		session.StorageBackend = storageBackend.String
		session.StorageKey = storageKey.String
		session.Committed = true
		session.Receipt = UploadReceipt{
			ImportID: spec.ImportID, State: ImportUploaded, SizeBytes: sizeBytes.Int64,
			SHA256: hex.EncodeToString(digest), UploadedAt: committedAt.Time,
		}
		if err := tx.Commit(); err != nil {
			return uploadSession{}, fmt.Errorf("finish upload replay lookup: %w", err)
		}
		return session, nil
	}
	if importState != ImportAwaitingUpload && importState != ImportUploading {
		return uploadSession{}, ErrUploadConflict
	}
	if !spec.StartedAt.Before(uploadExpiresAt) {
		if artifactID.Valid {
			if _, err := tx.ExecContext(ctx, `
				UPDATE open_aspm.raw_artifacts
				SET state = 'abandoned', upload_attempt_id = NULL,
				    upload_lease_expires_at = NULL, updated_at = $3
				WHERE workspace_id = $1 AND import_id = $2 AND state <> 'committed'`,
				spec.WorkspaceID, spec.ImportID, spec.StartedAt,
			); err != nil {
				return uploadSession{}, fmt.Errorf("abandon expired raw artifact: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_aspm.imports SET state = 'abandoned', updated_at = $3
			WHERE workspace_id = $1 AND id = $2`, spec.WorkspaceID, spec.ImportID, spec.StartedAt,
		); err != nil {
			return uploadSession{}, fmt.Errorf("abandon expired import upload: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return uploadSession{}, fmt.Errorf("commit expired import upload: %w", err)
		}
		return uploadSession{}, ErrUploadExpired
	}

	if !artifactID.Valid {
		_, err = tx.ExecContext(ctx, `
			INSERT INTO open_aspm.raw_artifacts (
				workspace_id, id, import_id, state, media_type_hint, storage_backend, storage_key,
				upload_attempt_id, upload_lease_expires_at, created_at, updated_at
			) VALUES ($1, $2, $3, 'uploading', $4, $5, $6, $7, $8, $9, $9)`,
			spec.WorkspaceID, spec.ArtifactID, spec.ImportID, rawArtifactMediaType,
			spec.StorageBackend, spec.StorageKey, spec.AttemptID, spec.LeaseExpiresAt, spec.StartedAt,
		)
		if err != nil {
			return uploadSession{}, classifyPostgresError("reserve raw artifact", err)
		}
		session.ArtifactID = spec.ArtifactID
		session.StorageBackend = spec.StorageBackend
		session.StorageKey = spec.StorageKey
	} else {
		session.ArtifactID = artifactID.String
		session.StorageBackend = storageBackend.String
		session.StorageKey = storageKey.String
		switch rawArtifactState(artifactState.String) {
		case rawArtifactUploading:
			if leaseExpiresAt.Valid && leaseExpiresAt.Time.After(spec.StartedAt) {
				return uploadSession{}, ErrUploadInProgress
			}
		case rawArtifactPending:
		default:
			return uploadSession{}, ErrUploadConflict
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE open_aspm.raw_artifacts
			SET state = 'uploading', upload_attempt_id = $4,
			    upload_lease_expires_at = $5, updated_at = $6
			WHERE workspace_id = $1 AND id = $2 AND import_id = $3
			  AND state IN ('pending', 'uploading')`,
			spec.WorkspaceID, session.ArtifactID, spec.ImportID, spec.AttemptID,
			spec.LeaseExpiresAt, spec.StartedAt,
		)
		if err != nil {
			return uploadSession{}, fmt.Errorf("renew raw artifact upload lease: %w", err)
		}
		if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
			return uploadSession{}, ErrUploadLeaseLost
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.imports SET state = 'uploading', updated_at = $3
		WHERE workspace_id = $1 AND id = $2`, spec.WorkspaceID, spec.ImportID, spec.StartedAt,
	); err != nil {
		return uploadSession{}, fmt.Errorf("mark import uploading: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return uploadSession{}, fmt.Errorf("commit raw artifact upload lease: %w", err)
	}
	session.AttemptID = spec.AttemptID
	return session, nil
}

// CommitUpload atomically records verified blob metadata and publishes the
// import as uploaded only for the current, unexpired attempt.
func (store *PostgresStore) CommitUpload(ctx context.Context, spec commitUploadSpec) (UploadReceipt, error) {
	digest, err := hex.DecodeString(spec.SHA256)
	if err != nil || len(digest) != 32 || spec.SizeBytes < 0 || spec.StorageVersion == "" ||
		spec.CommittedAt.IsZero() {
		return UploadReceipt{}, ErrInvalid
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return UploadReceipt{}, fmt.Errorf("begin raw artifact commit: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.raw_artifacts
		SET state = 'committed', upload_attempt_id = NULL,
		    upload_lease_expires_at = NULL, size_bytes = $5, sha256 = $6,
		    storage_version = $7, backend_version = NULLIF($8::varchar, ''),
		    updated_at = $9, committed_at = $9
		WHERE workspace_id = $1 AND import_id = $2 AND id = $3
		  AND state = 'uploading' AND upload_attempt_id = $4
		  AND upload_lease_expires_at >= $9`,
		spec.WorkspaceID, spec.ImportID, spec.ArtifactID, spec.AttemptID,
		spec.SizeBytes, digest, spec.StorageVersion, spec.BackendVersion, spec.CommittedAt,
	)
	if err != nil {
		return UploadReceipt{}, fmt.Errorf("commit raw artifact record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return UploadReceipt{}, ErrUploadLeaseLost
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE open_aspm.imports SET state = 'uploaded', updated_at = $3
		WHERE workspace_id = $1 AND id = $2 AND state = 'uploading'`,
		spec.WorkspaceID, spec.ImportID, spec.CommittedAt,
	)
	if err != nil {
		return UploadReceipt{}, fmt.Errorf("publish uploaded import: %w", err)
	}
	rows, err = result.RowsAffected()
	if err != nil || rows != 1 {
		return UploadReceipt{}, ErrUploadLeaseLost
	}
	if err := tx.Commit(); err != nil {
		return UploadReceipt{}, fmt.Errorf("commit uploaded import: %w", err)
	}
	return UploadReceipt{
		ImportID: spec.ImportID, State: ImportUploaded, SizeBytes: spec.SizeBytes,
		SHA256: spec.SHA256, UploadedAt: spec.CommittedAt,
	}, nil
}

// EndUpload either releases a retryable attempt back to pending or records a
// stable rejection. Attempt fencing prevents a stale request from changing a
// newer upload.
func (store *PostgresStore) EndUpload(ctx context.Context, spec endUploadSpec) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin ending raw artifact upload: %w", err)
	}
	defer tx.Rollback()
	artifactState := rawArtifactPending
	importState := ImportAwaitingUpload
	if spec.RejectCode != "" {
		artifactState = rawArtifactRejected
		importState = ImportRejected
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.raw_artifacts
		SET state = $5, upload_attempt_id = NULL, upload_lease_expires_at = NULL,
		    rejection_code = NULLIF($6::varchar, ''), updated_at = $7
		WHERE workspace_id = $1 AND import_id = $2 AND id = $3
		  AND state = 'uploading' AND upload_attempt_id = $4`,
		spec.WorkspaceID, spec.ImportID, spec.ArtifactID, spec.AttemptID,
		artifactState, spec.RejectCode, spec.EndedAt,
	)
	if err != nil {
		return fmt.Errorf("end raw artifact upload: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrUploadLeaseLost
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE open_aspm.imports SET state = $3, updated_at = $4
		WHERE workspace_id = $1 AND id = $2 AND state = 'uploading'`,
		spec.WorkspaceID, spec.ImportID, importState, spec.EndedAt,
	)
	if err != nil {
		return fmt.Errorf("end import upload: %w", err)
	}
	rows, err = result.RowsAffected()
	if err != nil || rows != 1 {
		return ErrUploadLeaseLost
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit end of raw artifact upload: %w", err)
	}
	return nil
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
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.raw_artifacts (
			workspace_id, id, import_id, state, media_type_hint,
			storage_backend, storage_key, created_at, updated_at
		) VALUES ($1, $2, $3, 'pending', $4, $5, $6, $7, $7)`,
		spec.Import.WorkspaceID, spec.ArtifactID, spec.Import.ID, rawArtifactMediaType,
		spec.StorageBackend, spec.StorageKey, spec.Import.CreatedAt,
	); err != nil {
		return Reservation{}, classifyPostgresError("insert raw artifact reservation", err)
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
