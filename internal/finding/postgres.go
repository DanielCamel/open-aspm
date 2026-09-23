package finding

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// PostgresStore owns writes to finding-context observation tables.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) (*PostgresStore, error) {
	if db == nil {
		return nil, ErrInvalid
	}
	return &PostgresStore{db: db}, nil
}

// RecordObservation inserts one immutable scanner statement and its structured
// locations and source fingerprints in one transaction. Exact delivery replay
// is idempotent; conflicting reuse of the source key is rejected.
func (store *PostgresStore) RecordObservation(ctx context.Context, spec ObservationSpec) (StoredObservation, error) {
	observation, fingerprint, err := canonicalizeObservation(spec)
	if err != nil {
		return StoredObservation{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return StoredObservation{}, fmt.Errorf("begin recording observation: %w", err)
	}
	defer tx.Rollback()

	var receivedAt time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT received_at
		FROM open_aspm.scans
		WHERE workspace_id = $1 AND id = $2 AND raw_artifact_id = $3`,
		spec.WorkspaceID, spec.ScanID, spec.RawArtifactID,
	).Scan(&receivedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredObservation{}, ErrScanNotFound
	}
	if err != nil {
		return StoredObservation{}, fmt.Errorf("read observation scan: %w", err)
	}
	receivedAt = receivedAt.UTC().Round(0)
	recordedAt := spec.RecordedAt.UTC().Round(0)
	if recordedAt.Before(receivedAt) {
		return StoredObservation{}, ErrInvalid
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.observations (
			workspace_id, id, scan_id, raw_artifact_id, source_result_index,
			parser_name, parser_version, source_pointer, source_guid,
			source_correlation_guid, source_rule_id, source_rule_index,
			source_level, source_kind, source_baseline_state, message_id,
			message_text, message_markdown, message_arguments, observed_at,
			received_at, recorded_at, record_fingerprint
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9::varchar, ''),
			NULLIF($10::varchar, ''), NULLIF($11::varchar, ''), $12,
			NULLIF($13::varchar, ''), NULLIF($14::varchar, ''), NULLIF($15::varchar, ''),
			NULLIF($16::varchar, ''), NULLIF($17::text, ''), NULLIF($18::text, ''),
			$19, $20, $21, $22, $23
		)
		ON CONFLICT (
			workspace_id, scan_id, parser_name, parser_version, source_result_index
		) DO NOTHING`,
		observation.WorkspaceID, spec.ID, observation.ScanID, observation.RawArtifactID,
		observation.SourceResultIndex, observation.ParserName, observation.ParserVersion,
		observation.SourcePointer, observation.SourceGUID, observation.SourceCorrelationGUID,
		observation.SourceRuleID, observation.SourceRuleIndex, observation.SourceSeverity,
		observation.SourceKind, observation.SourceBaselineState, observation.MessageID,
		observation.MessageText, observation.MessageMarkdown, observation.MessageArguments,
		observation.ObservedAt, receivedAt, recordedAt, fingerprint[:],
	)
	if err != nil {
		return StoredObservation{}, fmt.Errorf("insert observation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return StoredObservation{}, fmt.Errorf("read observation insert result: %w", err)
	}
	if rows == 0 {
		stored, existingFingerprint, err := selectStoredObservation(ctx, tx, spec)
		if err != nil {
			return StoredObservation{}, err
		}
		if !bytes.Equal(existingFingerprint, fingerprint[:]) {
			return StoredObservation{}, ErrObservationConflict
		}
		if err := tx.Commit(); err != nil {
			return StoredObservation{}, fmt.Errorf("finish observation replay: %w", err)
		}
		return stored, nil
	}

	for _, location := range observation.Locations {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO open_aspm.observation_locations (
				workspace_id, observation_id, ordinal, source_pointer, uri,
				uri_base_id, artifact_index, start_line, start_column, end_line, end_column
			) VALUES (
				$1, $2, $3, $4, NULLIF($5::text, ''), NULLIF($6::varchar, ''),
				$7, $8, $9, $10, $11
			)`,
			spec.WorkspaceID, spec.ID, location.Ordinal, location.SourcePointer, location.URI,
			location.URIBaseID, location.ArtifactIndex, location.StartLine, location.StartColumn,
			location.EndLine, location.EndColumn,
		); err != nil {
			return StoredObservation{}, fmt.Errorf("insert observation location: %w", err)
		}
	}
	for _, sourceFingerprint := range observation.Fingerprints {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO open_aspm.observation_fingerprints (
				workspace_id, observation_id, kind, name, value
			) VALUES ($1, $2, $3, $4, $5)`,
			spec.WorkspaceID, spec.ID, sourceFingerprint.Kind,
			sourceFingerprint.Name, sourceFingerprint.Value,
		); err != nil {
			return StoredObservation{}, fmt.Errorf("insert observation fingerprint: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return StoredObservation{}, fmt.Errorf("commit observation: %w", err)
	}
	return StoredObservation{
		ID: spec.ID, WorkspaceID: spec.WorkspaceID, ScanID: spec.ScanID,
		RawArtifactID: spec.RawArtifactID, ReceivedAt: receivedAt,
		RecordedAt: recordedAt, Created: true,
	}, nil
}

func selectStoredObservation(
	ctx context.Context,
	tx *sql.Tx,
	spec ObservationSpec,
) (StoredObservation, []byte, error) {
	var stored StoredObservation
	var fingerprint []byte
	err := tx.QueryRowContext(ctx, `
		SELECT id, workspace_id, scan_id, raw_artifact_id, received_at, recorded_at,
		       record_fingerprint
		FROM open_aspm.observations
		WHERE workspace_id = $1 AND scan_id = $2 AND parser_name = $3
		  AND parser_version = $4 AND source_result_index = $5`,
		spec.WorkspaceID, spec.ScanID, spec.ParserName, spec.ParserVersion, spec.SourceResultIndex,
	).Scan(
		&stored.ID, &stored.WorkspaceID, &stored.ScanID, &stored.RawArtifactID,
		&stored.ReceivedAt, &stored.RecordedAt, &fingerprint,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredObservation{}, nil, ErrObservationConflict
	}
	if err != nil {
		return StoredObservation{}, nil, fmt.Errorf("read existing observation: %w", err)
	}
	return stored, fingerprint, nil
}
