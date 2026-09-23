package ingestion

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RecordScan immutably persists one source run and its versioned scope. The
// source key is workspace + import + run index; an exact replay returns the
// original record, while different content for that key is rejected.
func (store *PostgresStore) RecordScan(ctx context.Context, spec ScanSpec) (StoredScan, error) {
	scan, scope, scanFingerprint, scopeFingerprint, err := canonicalizeScanSpec(spec)
	if err != nil {
		return StoredScan{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return StoredScan{}, fmt.Errorf("begin recording scan: %w", err)
	}
	defer tx.Rollback()

	var receivedAt time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT artifact.committed_at
		FROM open_aspm.imports AS imp
		JOIN open_aspm.raw_artifacts AS artifact
		  ON artifact.workspace_id = imp.workspace_id AND artifact.import_id = imp.id
		WHERE imp.workspace_id = $1 AND imp.id = $2 AND imp.application_id = $3
		  AND artifact.id = $4 AND artifact.state = 'committed'
		FOR SHARE OF imp, artifact`,
		spec.WorkspaceID, spec.ImportID, spec.ApplicationID, spec.RawArtifactID,
	).Scan(&receivedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredScan{}, ErrScanSourceNotFound
	}
	if err != nil {
		return StoredScan{}, fmt.Errorf("read scan source evidence: %w", err)
	}
	receivedAt = receivedAt.UTC().Round(0)
	recordedAt := spec.RecordedAt.UTC().Round(0)
	if recordedAt.Before(receivedAt) {
		return StoredScan{}, ErrInvalid
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.scans (
			workspace_id, id, import_id, application_id, raw_artifact_id,
			source_run_index, result, completeness, scanner_name, scanner_full_name,
			scanner_version, scanner_semantic_version, automation_id, automation_guid,
			automation_correlation_guid, parser_name, parser_version, source_pointer,
			source_started_at, source_ended_at, received_at, recorded_at, record_fingerprint
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, NULLIF($10::varchar, ''),
			NULLIF($11::varchar, ''), NULLIF($12::varchar, ''), NULLIF($13::varchar, ''),
			NULLIF($14::varchar, ''), NULLIF($15::varchar, ''), $16, $17, $18,
			$19, $20, $21, $22, $23
		)
		ON CONFLICT (workspace_id, import_id, source_run_index) DO NOTHING`,
		scan.WorkspaceID, spec.ID, scan.ImportID, scan.ApplicationID, scan.RawArtifactID,
		scan.SourceRunIndex, scan.Result, scan.Completeness, scan.ScannerName, scan.ScannerFullName,
		scan.ScannerVersion, scan.ScannerSemanticVersion, scan.AutomationID, scan.AutomationGUID,
		scan.AutomationCorrelationGUID, scan.ParserName, scan.ParserVersion, scan.SourcePointer,
		scan.SourceStartedAt, scan.SourceEndedAt, receivedAt, recordedAt, scanFingerprint[:],
	)
	if err != nil {
		return StoredScan{}, classifyPostgresError("insert scan", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return StoredScan{}, fmt.Errorf("read scan insert result: %w", err)
	}
	if rows == 0 {
		stored, existingScanFingerprint, existingScopeFingerprint, err := selectStoredScan(ctx, tx, spec)
		if err != nil {
			return StoredScan{}, err
		}
		if !bytes.Equal(existingScanFingerprint, scanFingerprint[:]) ||
			!bytes.Equal(existingScopeFingerprint, scopeFingerprint[:]) {
			return StoredScan{}, ErrScanConflict
		}
		if err := tx.Commit(); err != nil {
			return StoredScan{}, fmt.Errorf("finish scan replay: %w", err)
		}
		return stored, nil
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.scan_scopes (
			workspace_id, scan_id, application_id, schema_version, scanner_family,
			scanner_instance_id, scanner_configuration_id, scanner_configuration_hash,
			analysis_kind, coverage_metadata, scope_fingerprint, created_at
		) VALUES (
			$1, $2, $3, $4, $5, NULLIF($6::varchar, ''), NULLIF($7::varchar, ''),
			NULLIF($8::varchar, ''), $9, $10, $11, $12
		)`,
		spec.WorkspaceID, spec.ID, spec.ApplicationID, scope.SchemaVersion, scope.ScannerFamily,
		scope.ScannerInstanceID, scope.ScannerConfigurationID, scope.ScannerConfigurationHash,
		scope.AnalysisKind, string(scope.CoverageMetadata), scopeFingerprint[:], recordedAt,
	); err != nil {
		return StoredScan{}, classifyPostgresError("insert scan scope", err)
	}
	if err := tx.Commit(); err != nil {
		return StoredScan{}, fmt.Errorf("commit scan: %w", err)
	}
	return StoredScan{
		ID: spec.ID, WorkspaceID: spec.WorkspaceID, ImportID: spec.ImportID,
		ApplicationID: spec.ApplicationID, RawArtifactID: spec.RawArtifactID,
		ReceivedAt: receivedAt, RecordedAt: recordedAt, Created: true,
	}, nil
}

func selectStoredScan(
	ctx context.Context,
	tx *sql.Tx,
	spec ScanSpec,
) (StoredScan, []byte, []byte, error) {
	var stored StoredScan
	var scanFingerprint, scopeFingerprint []byte
	err := tx.QueryRowContext(ctx, `
		SELECT scan.id, scan.workspace_id, scan.import_id, scan.application_id,
		       scan.raw_artifact_id, scan.received_at, scan.recorded_at,
		       scan.record_fingerprint, scope.scope_fingerprint
		FROM open_aspm.scans AS scan
		JOIN open_aspm.scan_scopes AS scope
		  ON scope.workspace_id = scan.workspace_id AND scope.scan_id = scan.id
		WHERE scan.workspace_id = $1 AND scan.import_id = $2 AND scan.source_run_index = $3`,
		spec.WorkspaceID, spec.ImportID, spec.SourceRunIndex,
	).Scan(
		&stored.ID, &stored.WorkspaceID, &stored.ImportID, &stored.ApplicationID,
		&stored.RawArtifactID, &stored.ReceivedAt, &stored.RecordedAt,
		&scanFingerprint, &scopeFingerprint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredScan{}, nil, nil, ErrScanConflict
	}
	if err != nil {
		return StoredScan{}, nil, nil, fmt.Errorf("read existing scan: %w", err)
	}
	return stored, scanFingerprint, scopeFingerprint, nil
}
