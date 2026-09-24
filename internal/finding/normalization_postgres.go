package finding

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// RecordNormalization immutably records one versioned interpretation of an
// Observation. Exact replay returns the retained row; a different result under
// the same normalizer version is rejected.
func (store *PostgresStore) RecordNormalization(
	ctx context.Context,
	spec NormalizationSpec,
) (StoredNormalization, error) {
	canonical, fingerprint, err := canonicalizeNormalization(spec)
	if err != nil {
		return StoredNormalization{}, err
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return StoredNormalization{}, fmt.Errorf("begin recording observation normalization: %w", err)
	}
	defer tx.Rollback()

	var observationRecordedAt sql.NullTime
	err = tx.QueryRowContext(ctx, `
		SELECT recorded_at
		FROM open_aspm.observations
		WHERE workspace_id = $1 AND id = $2`,
		spec.WorkspaceID, spec.ObservationID,
	).Scan(&observationRecordedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return StoredNormalization{}, ErrObservationNotFound
	}
	if err != nil {
		return StoredNormalization{}, fmt.Errorf("read normalization source observation: %w", err)
	}
	normalizedAt := spec.NormalizedAt.UTC().Round(0)
	if !observationRecordedAt.Valid || normalizedAt.Before(observationRecordedAt.Time.UTC().Round(0)) {
		return StoredNormalization{}, ErrNormalizationInvalid
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.observation_normalizations (
			workspace_id, observation_id, normalizer_name, normalizer_version,
			category, severity, rule_kind, rule_id, location_kind, location_uri,
			location_uri_base_id, location_start_line, location_start_column,
			location_end_line, location_end_column, normalized_at, record_fingerprint
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, NULLIF($8::varchar, ''), $9,
			NULLIF($10::text, ''), NULLIF($11::varchar, ''), $12, $13, $14, $15, $16, $17
		)
		ON CONFLICT (
			workspace_id, observation_id, normalizer_name, normalizer_version
		) DO NOTHING`,
		canonical.WorkspaceID, canonical.ObservationID, canonical.NormalizerName,
		canonical.NormalizerVersion, canonical.Category, canonical.Severity,
		canonical.RuleKind, canonical.RuleID, canonical.Location.Kind,
		canonical.Location.URI, canonical.Location.URIBaseID,
		canonical.Location.StartLine, canonical.Location.StartColumn,
		canonical.Location.EndLine, canonical.Location.EndColumn,
		normalizedAt, fingerprint[:],
	)
	if err != nil {
		return StoredNormalization{}, fmt.Errorf("insert observation normalization: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return StoredNormalization{}, fmt.Errorf("read observation normalization insert result: %w", err)
	}
	if rows == 0 {
		var stored StoredNormalization
		var existingFingerprint []byte
		err := tx.QueryRowContext(ctx, `
			SELECT workspace_id, observation_id, normalizer_name, normalizer_version,
			       normalized_at, record_fingerprint
			FROM open_aspm.observation_normalizations
			WHERE workspace_id = $1 AND observation_id = $2
			  AND normalizer_name = $3 AND normalizer_version = $4`,
			spec.WorkspaceID, spec.ObservationID, spec.NormalizerName, spec.NormalizerVersion,
		).Scan(
			&stored.WorkspaceID, &stored.ObservationID, &stored.NormalizerName,
			&stored.NormalizerVersion, &stored.NormalizedAt, &existingFingerprint,
		)
		if err != nil {
			return StoredNormalization{}, fmt.Errorf("read existing observation normalization: %w", err)
		}
		if !bytes.Equal(existingFingerprint, fingerprint[:]) {
			return StoredNormalization{}, ErrNormalizationConflict
		}
		if err := tx.Commit(); err != nil {
			return StoredNormalization{}, fmt.Errorf("finish observation normalization replay: %w", err)
		}
		return stored, nil
	}
	if err := tx.Commit(); err != nil {
		return StoredNormalization{}, fmt.Errorf("commit observation normalization: %w", err)
	}
	return StoredNormalization{
		WorkspaceID: spec.WorkspaceID, ObservationID: spec.ObservationID,
		NormalizerName: spec.NormalizerName, NormalizerVersion: spec.NormalizerVersion,
		NormalizedAt: normalizedAt, Created: true,
	}, nil
}
