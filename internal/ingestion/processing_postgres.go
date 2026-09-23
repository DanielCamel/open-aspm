package ingestion

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ResolveProcessingTarget returns only the application scope and public state
// needed to authorize a delayed import job. It never returns a storage key.
func (store *PostgresStore) ResolveProcessingTarget(
	ctx context.Context,
	identity ProcessingIdentity,
) (ProcessingTarget, error) {
	if err := validateProcessingIdentity(identity); err != nil {
		return ProcessingTarget{}, err
	}
	var target ProcessingTarget
	var failureCode sql.NullString
	err := store.db.QueryRowContext(ctx, `
		SELECT op.workspace_id, op.id, op.import_id, imp.application_id, op.state, op.failure_code
		FROM open_aspm.operations AS op
		JOIN open_aspm.imports AS imp
		  ON imp.workspace_id = op.workspace_id AND imp.id = op.import_id
		WHERE op.workspace_id = $1 AND op.id = $2 AND op.import_id = $3
		  AND op.created_by_principal_id = $4 AND op.kind = $5`,
		identity.WorkspaceID, identity.OperationID, identity.ImportID,
		identity.PrincipalID, ImportProcessJobKind,
	).Scan(
		&target.WorkspaceID, &target.OperationID, &target.ImportID,
		&target.ApplicationID, &target.State, &failureCode,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ProcessingTarget{}, ErrProcessingNotFound
	}
	if err != nil {
		return ProcessingTarget{}, fmt.Errorf("resolve import processing target: %w", err)
	}
	target.FailureCode = failureCode.String
	return target, nil
}

// BeginProcessing moves an authorized queued import and operation into their
// public processing states and returns committed evidence metadata. Re-entry
// while running is safe; terminal state is returned without evidence access.
func (store *PostgresStore) BeginProcessing(
	ctx context.Context,
	identity ProcessingIdentity,
	startedAt time.Time,
) (ProcessingSource, error) {
	if err := validateProcessingIdentity(identity); err != nil || startedAt.IsZero() {
		return ProcessingSource{}, ErrInvalid
	}
	startedAt = startedAt.UTC().Round(0)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return ProcessingSource{}, fmt.Errorf("begin import processing: %w", err)
	}
	defer tx.Rollback()

	var source ProcessingSource
	var formatVersion, backendVersion sql.NullString
	var digest []byte
	var importState ImportState
	var failureCode sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT op.workspace_id, op.id, op.import_id, imp.application_id,
		       op.state, op.failure_code, imp.state,
		       imp.report_format_name, imp.report_format_version, imp.max_bytes,
		       artifact.id, artifact.storage_backend, artifact.storage_key,
		       artifact.storage_version, artifact.backend_version,
		       artifact.size_bytes, artifact.sha256, artifact.committed_at
		FROM open_aspm.operations AS op
		JOIN open_aspm.imports AS imp
		  ON imp.workspace_id = op.workspace_id AND imp.id = op.import_id
		JOIN open_aspm.raw_artifacts AS artifact
		  ON artifact.workspace_id = imp.workspace_id AND artifact.import_id = imp.id
		 AND artifact.state = 'committed'
		WHERE op.workspace_id = $1 AND op.id = $2 AND op.import_id = $3
		  AND op.created_by_principal_id = $4 AND op.kind = $5
		FOR UPDATE OF op, imp`,
		identity.WorkspaceID, identity.OperationID, identity.ImportID,
		identity.PrincipalID, ImportProcessJobKind,
	).Scan(
		&source.WorkspaceID, &source.OperationID, &source.ImportID, &source.ApplicationID,
		&source.State, &failureCode, &importState,
		&source.ReportFormat.Name, &formatVersion, &source.MaxBytes,
		&source.RawArtifactID, &source.StorageBackend, &source.StorageKey,
		&source.StorageVersion, &backendVersion,
		&source.SizeBytes, &digest, &source.ReceivedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ProcessingSource{}, ErrProcessingNotFound
	}
	if err != nil {
		return ProcessingSource{}, fmt.Errorf("lock import processing source: %w", err)
	}
	source.FailureCode = failureCode.String
	if source.State == OperationSucceeded || source.State == OperationFailed || source.State == OperationCancelled {
		if err := tx.Commit(); err != nil {
			return ProcessingSource{}, fmt.Errorf("finish terminal import processing lookup: %w", err)
		}
		return source, nil
	}
	if (source.State != OperationQueued && source.State != OperationRunning) ||
		(importState != ImportQueued && importState != ImportProcessing) ||
		source.RawArtifactID == "" || source.SizeBytes < 0 || len(digest) != 32 {
		return ProcessingSource{}, ErrProcessingConflict
	}
	source.ReportFormat.Version = formatVersion.String
	source.BackendVersion = backendVersion.String
	source.SHA256 = hex.EncodeToString(digest)

	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.operations
		SET state = 'running', started_at = COALESCE(started_at, $3), updated_at = $3
		WHERE workspace_id = $1 AND id = $2 AND state IN ('queued', 'running')`,
		identity.WorkspaceID, identity.OperationID, startedAt,
	); err != nil {
		return ProcessingSource{}, fmt.Errorf("mark operation running: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.imports
		SET state = 'processing', updated_at = $3
		WHERE workspace_id = $1 AND id = $2 AND state IN ('queued', 'processing')`,
		identity.WorkspaceID, identity.ImportID, startedAt,
	); err != nil {
		return ProcessingSource{}, fmt.Errorf("mark import processing: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ProcessingSource{}, fmt.Errorf("commit import processing start: %w", err)
	}
	source.State = OperationRunning
	return source, nil
}

// RecordParseOutput stores deterministic parser diagnostics without mutating a
// prior interpretation produced by the same parser version.
func (store *PostgresStore) RecordParseOutput(ctx context.Context, spec ParseOutputSpec) error {
	warnings, fingerprint, err := canonicalizeParseOutput(spec)
	if err != nil {
		return err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin recording parser output: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.import_parse_outputs (
			workspace_id, import_id, raw_artifact_id, format_name, format_version,
			parser_name, parser_version, warning_count, warnings_truncated,
			recorded_at, record_fingerprint
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (workspace_id, import_id, parser_name, parser_version) DO NOTHING`,
		spec.WorkspaceID, spec.ImportID, spec.RawArtifactID, spec.FormatName, spec.FormatVersion,
		spec.ParserName, spec.ParserVersion, len(warnings), spec.WarningsTruncated,
		spec.RecordedAt.UTC().Round(0), fingerprint[:],
	)
	if err != nil {
		return fmt.Errorf("insert parser output: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read parser output insert result: %w", err)
	}
	if rows == 0 {
		var existing []byte
		err := tx.QueryRowContext(ctx, `
			SELECT record_fingerprint
			FROM open_aspm.import_parse_outputs
			WHERE workspace_id = $1 AND import_id = $2
			  AND parser_name = $3 AND parser_version = $4`,
			spec.WorkspaceID, spec.ImportID, spec.ParserName, spec.ParserVersion,
		).Scan(&existing)
		if err != nil {
			return fmt.Errorf("read existing parser output: %w", err)
		}
		if !bytes.Equal(existing, fingerprint[:]) {
			return ErrParseOutputConflict
		}
		return tx.Commit()
	}
	for index, warning := range warnings {
		fields := warning.Fields
		if fields == nil {
			fields = []string{}
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO open_aspm.import_parse_warnings (
				workspace_id, import_id, parser_name, parser_version, warning_index,
				code, source_pointer, field_count, fields, fields_truncated
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			spec.WorkspaceID, spec.ImportID, spec.ParserName, spec.ParserVersion, index,
			warning.Code, warning.SourcePointer, warning.FieldCount, fields, warning.FieldsTruncated,
		); err != nil {
			return fmt.Errorf("insert parser warning: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit parser output: %w", err)
	}
	return nil
}

// CompleteProcessing atomically publishes the terminal successful Import and
// Operation result. Replaying the same result is a no-op.
func (store *PostgresStore) CompleteProcessing(
	ctx context.Context,
	identity ProcessingIdentity,
	result ProcessingResult,
	finishedAt time.Time,
) error {
	if err := validateProcessingIdentity(identity); err != nil || result.ScansCreated < 0 || finishedAt.IsZero() {
		return ErrInvalid
	}
	operationResult, err := json.Marshal(struct {
		ImportID     string `json:"import_id"`
		ImportState  string `json:"import_state"`
		ScansCreated int    `json:"scans_created"`
	}{ImportID: identity.ImportID, ImportState: string(ImportSucceeded), ScansCreated: result.ScansCreated})
	if err != nil {
		return fmt.Errorf("encode import processing result: %w", err)
	}
	finishedAt = finishedAt.UTC().Round(0)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin completing import processing: %w", err)
	}
	defer tx.Rollback()
	var operationState OperationState
	var importState ImportState
	var existingResult []byte
	err = tx.QueryRowContext(ctx, `
		SELECT op.state, op.result, imp.state
		FROM open_aspm.operations AS op
		JOIN open_aspm.imports AS imp
		  ON imp.workspace_id = op.workspace_id AND imp.id = op.import_id
		WHERE op.workspace_id = $1 AND op.id = $2 AND op.import_id = $3
		  AND op.created_by_principal_id = $4 AND op.kind = $5
		FOR UPDATE OF op, imp`,
		identity.WorkspaceID, identity.OperationID, identity.ImportID,
		identity.PrincipalID, ImportProcessJobKind,
	).Scan(&operationState, &existingResult, &importState)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProcessingNotFound
	}
	if err != nil {
		return fmt.Errorf("lock successful import processing: %w", err)
	}
	if operationState == OperationSucceeded {
		var existing struct {
			ImportID     string `json:"import_id"`
			ImportState  string `json:"import_state"`
			ScansCreated int    `json:"scans_created"`
		}
		if json.Unmarshal(existingResult, &existing) != nil || existing.ImportID != identity.ImportID ||
			existing.ImportState != string(ImportSucceeded) || existing.ScansCreated != result.ScansCreated ||
			importState != ImportSucceeded {
			return ErrProcessingConflict
		}
		return tx.Commit()
	}
	if operationState != OperationRunning || importState != ImportProcessing {
		return ErrProcessingConflict
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.imports SET state = 'succeeded', updated_at = $3
		WHERE workspace_id = $1 AND id = $2 AND state = 'processing'`,
		identity.WorkspaceID, identity.ImportID, finishedAt,
	); err != nil {
		return fmt.Errorf("complete processed import: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.operations
		SET state = 'succeeded', result = $3, updated_at = $4, finished_at = $4
		WHERE workspace_id = $1 AND id = $2 AND state = 'running'`,
		identity.WorkspaceID, identity.OperationID, operationResult, finishedAt,
	); err != nil {
		return fmt.Errorf("complete import operation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit successful import processing: %w", err)
	}
	return nil
}

// FailProcessing atomically publishes a sanitized terminal failure for both
// the Import and its public Operation. Replaying the same failure is safe.
func (store *PostgresStore) FailProcessing(
	ctx context.Context,
	identity ProcessingIdentity,
	failure ProcessingFailure,
	finishedAt time.Time,
) error {
	if err := validateProcessingIdentity(identity); err != nil || !validFailure(failure) || finishedAt.IsZero() {
		return ErrInvalid
	}
	finishedAt = finishedAt.UTC().Round(0)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failing import processing: %w", err)
	}
	defer tx.Rollback()
	var operationState OperationState
	var importState ImportState
	var existingCode sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT op.state, op.failure_code, imp.state
		FROM open_aspm.operations AS op
		JOIN open_aspm.imports AS imp
		  ON imp.workspace_id = op.workspace_id AND imp.id = op.import_id
		WHERE op.workspace_id = $1 AND op.id = $2 AND op.import_id = $3
		  AND op.created_by_principal_id = $4 AND op.kind = $5
		FOR UPDATE OF op, imp`,
		identity.WorkspaceID, identity.OperationID, identity.ImportID,
		identity.PrincipalID, ImportProcessJobKind,
	).Scan(&operationState, &existingCode, &importState)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProcessingNotFound
	}
	if err != nil {
		return fmt.Errorf("lock failed import processing: %w", err)
	}
	if operationState == OperationFailed {
		if existingCode.String != failure.Code || importState != ImportFailed {
			return ErrProcessingConflict
		}
		return tx.Commit()
	}
	if (operationState != OperationQueued && operationState != OperationRunning) ||
		(importState != ImportQueued && importState != ImportProcessing) {
		return ErrProcessingConflict
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.imports SET state = 'failed', updated_at = $3
		WHERE workspace_id = $1 AND id = $2 AND state IN ('queued', 'processing')`,
		identity.WorkspaceID, identity.ImportID, finishedAt,
	); err != nil {
		return fmt.Errorf("fail processed import: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.operations
		SET state = 'failed', result = NULL, failure_code = $3,
		    failure_title = $4, failure_detail = NULLIF($5::varchar, ''),
		    updated_at = $6, finished_at = $6
		WHERE workspace_id = $1 AND id = $2 AND state IN ('queued', 'running')`,
		identity.WorkspaceID, identity.OperationID, failure.Code,
		failure.Title, failure.Detail, finishedAt,
	); err != nil {
		return fmt.Errorf("fail import operation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed import processing: %w", err)
	}
	return nil
}
