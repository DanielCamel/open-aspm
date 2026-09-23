-- +goose Up
CREATE TABLE open_aspm.raw_artifacts (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    import_id varchar(128) NOT NULL,
    state varchar(32) NOT NULL,
    media_type_hint varchar(128) NOT NULL,
    storage_backend varchar(64) NOT NULL,
    storage_key varchar(128) NOT NULL,
    upload_attempt_id varchar(128),
    upload_lease_expires_at timestamptz,
    size_bytes bigint,
    sha256 bytea,
    storage_version varchar(128),
    backend_version varchar(1024),
    rejection_code varchar(64),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    committed_at timestamptz,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, import_id),
    UNIQUE (storage_backend, storage_key),
    CONSTRAINT raw_artifacts_import_fk
        FOREIGN KEY (workspace_id, import_id)
        REFERENCES open_aspm.imports (workspace_id, id),
    CONSTRAINT raw_artifacts_id_length CHECK (length(id) BETWEEN 3 AND 128),
    CONSTRAINT raw_artifacts_state_known CHECK (
        state IN (
            'pending', 'uploading', 'committed', 'rejected',
            'abandoned', 'deletion_pending', 'deleted'
        )
    ),
    CONSTRAINT raw_artifacts_media_type_hint_length
        CHECK (length(media_type_hint) BETWEEN 1 AND 128),
    CONSTRAINT raw_artifacts_storage_backend_format CHECK (
        storage_backend ~ '^[a-z][a-z0-9._-]{0,63}$'
    ),
    CONSTRAINT raw_artifacts_storage_key_format CHECK (
        storage_key ~ '^blb_[a-z2-7]{52}$'
    ),
    CONSTRAINT raw_artifacts_upload_lease_consistent CHECK (
        (state = 'uploading') =
        (upload_attempt_id IS NOT NULL AND upload_lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT raw_artifacts_size_valid CHECK (size_bytes IS NULL OR size_bytes >= 0),
    CONSTRAINT raw_artifacts_sha256_length CHECK (
        sha256 IS NULL OR octet_length(sha256) = 32
    ),
    CONSTRAINT raw_artifacts_committed_metadata_consistent CHECK (
        (state IN ('committed', 'deletion_pending', 'deleted')) =
        (size_bytes IS NOT NULL AND sha256 IS NOT NULL AND
         storage_version IS NOT NULL AND committed_at IS NOT NULL)
    ),
    CONSTRAINT raw_artifacts_rejection_consistent CHECK (
        (state = 'rejected') = (rejection_code IS NOT NULL)
    ),
    CONSTRAINT raw_artifacts_updated_at_valid CHECK (updated_at >= created_at),
    CONSTRAINT raw_artifacts_committed_at_valid CHECK (
        committed_at IS NULL OR committed_at >= created_at
    )
);

CREATE INDEX raw_artifacts_import_state_idx
    ON open_aspm.raw_artifacts (workspace_id, import_id, state);
