-- +goose Up
CREATE TABLE open_aspm.operations (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    import_id varchar(128) NOT NULL,
    created_by_principal_id varchar(128) NOT NULL,
    kind varchar(64) NOT NULL,
    state varchar(32) NOT NULL,
    result jsonb,
    failure_code varchar(128),
    failure_title varchar(255),
    failure_detail varchar(2048),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, import_id, kind),
    CONSTRAINT operations_import_fk
        FOREIGN KEY (workspace_id, import_id)
        REFERENCES open_aspm.imports (workspace_id, id),
    CONSTRAINT operations_id_length CHECK (length(id) BETWEEN 3 AND 128),
    CONSTRAINT operations_principal_id_length
        CHECK (length(created_by_principal_id) BETWEEN 3 AND 128),
    CONSTRAINT operations_kind_known CHECK (kind = 'import.process'),
    CONSTRAINT operations_state_known CHECK (
        state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT operations_result_object CHECK (
        result IS NULL OR jsonb_typeof(result) = 'object'
    ),
    CONSTRAINT operations_result_bounded CHECK (
        result IS NULL OR pg_column_size(result) <= 65536
    ),
    CONSTRAINT operations_result_consistent CHECK (
        (state = 'succeeded') = (result IS NOT NULL)
    ),
    CONSTRAINT operations_failure_consistent CHECK (
        (state = 'failed') = (failure_code IS NOT NULL AND failure_title IS NOT NULL) AND
        (state = 'failed' OR
         (failure_code IS NULL AND failure_title IS NULL AND failure_detail IS NULL))
    ),
    CONSTRAINT operations_failure_code_format CHECK (
        failure_code IS NULL OR failure_code ~ '^[a-z][a-z0-9.-]{0,127}$'
    ),
    CONSTRAINT operations_running_started CHECK (
        state <> 'running' OR started_at IS NOT NULL
    ),
    CONSTRAINT operations_finished_at_consistent CHECK (
        (state IN ('succeeded', 'failed', 'cancelled')) = (finished_at IS NOT NULL)
    ),
    CONSTRAINT operations_updated_at_valid CHECK (updated_at >= created_at),
    CONSTRAINT operations_started_at_valid CHECK (
        started_at IS NULL OR started_at >= created_at
    ),
    CONSTRAINT operations_finished_at_valid CHECK (
        finished_at IS NULL OR
        (finished_at >= created_at AND (started_at IS NULL OR finished_at >= started_at))
    )
);

CREATE INDEX operations_creator_idx
    ON open_aspm.operations (workspace_id, created_by_principal_id, id);

CREATE TABLE open_aspm.import_complete_idempotency (
    workspace_id varchar(128) NOT NULL,
    principal_id varchar(128) NOT NULL,
    api_major_version smallint NOT NULL,
    operation varchar(64) NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_fingerprint bytea NOT NULL,
    operation_id varchar(128) NOT NULL,
    completed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (
        workspace_id, principal_id, api_major_version, operation, idempotency_key
    ),
    CONSTRAINT import_complete_idempotency_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES open_aspm.workspaces (id),
    CONSTRAINT import_complete_idempotency_operation_fk
        FOREIGN KEY (workspace_id, operation_id)
        REFERENCES open_aspm.operations (workspace_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT import_complete_idempotency_principal_length
        CHECK (length(principal_id) BETWEEN 3 AND 128),
    CONSTRAINT import_complete_idempotency_api_version_positive
        CHECK (api_major_version > 0),
    CONSTRAINT import_complete_idempotency_operation_format
        CHECK (operation ~ '^[a-z][a-z0-9._:-]{0,63}$'),
    CONSTRAINT import_complete_idempotency_key_format
        CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT import_complete_idempotency_fingerprint_length
        CHECK (octet_length(request_fingerprint) = 32),
    CONSTRAINT import_complete_idempotency_retention CHECK (
        expires_at >= completed_at + interval '24 hours'
    )
);

-- Migration 00002 allowed optional operation IDs before the public operation
-- table existed. NOT VALID preserves those legacy rows while enforcing the
-- relationship for every new job written after this migration.
ALTER TABLE open_aspm.jobs
    ADD CONSTRAINT jobs_operation_fk
    FOREIGN KEY (workspace_id, operation_id)
    REFERENCES open_aspm.operations (workspace_id, id)
    NOT VALID;
