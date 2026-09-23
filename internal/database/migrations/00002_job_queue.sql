-- +goose Up
CREATE TABLE open_aspm.workspaces (
    id varchar(128) PRIMARY KEY,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT workspaces_id_length CHECK (length(id) BETWEEN 3 AND 128)
);

CREATE TABLE open_aspm.jobs (
    id varchar(128) PRIMARY KEY,
    workspace_id varchar(128) NOT NULL,
    operation_id varchar(128),
    initiating_principal_id varchar(128) NOT NULL,
    system_capability varchar(128) NOT NULL,
    queue varchar(128) NOT NULL,
    kind varchar(128) NOT NULL,
    schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_fingerprint bytea NOT NULL,
    state varchar(32) NOT NULL DEFAULT 'queued',
    priority smallint NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL,
    attempt_count smallint NOT NULL DEFAULT 0,
    max_attempts smallint NOT NULL,
    lease_owner varchar(128),
    lease_token_hash bytea,
    lease_expires_at timestamptz,
    heartbeat_at timestamptz,
    cancellation_requested_at timestamptz,
    last_error_class varchar(64),
    last_error_code varchar(128),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    started_at timestamptz,
    completed_at timestamptz,
    CONSTRAINT jobs_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES open_aspm.workspaces (id),
    CONSTRAINT jobs_workspace_id_unique UNIQUE (workspace_id, id),
    CONSTRAINT jobs_operation_id_length
        CHECK (operation_id IS NULL OR length(operation_id) BETWEEN 3 AND 128),
    CONSTRAINT jobs_initiating_principal_id_length
        CHECK (length(initiating_principal_id) BETWEEN 3 AND 128),
    CONSTRAINT jobs_system_capability_format
        CHECK (system_capability ~ '^[a-z][a-z0-9._:-]{0,127}$'),
    CONSTRAINT jobs_queue_format CHECK (queue ~ '^[a-z][a-z0-9._-]{0,127}$'),
    CONSTRAINT jobs_kind_format CHECK (kind ~ '^[a-z][a-z0-9._-]{0,127}$'),
    CONSTRAINT jobs_schema_version_positive CHECK (schema_version > 0),
    CONSTRAINT jobs_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_payload_bounded CHECK (pg_column_size(payload) <= 65536),
    CONSTRAINT jobs_idempotency_key_format
        CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT jobs_request_fingerprint_length
        CHECK (octet_length(request_fingerprint) = 32),
    CONSTRAINT jobs_state_known CHECK (
        state IN (
            'queued', 'leased', 'running', 'retry_wait',
            'succeeded', 'dead_letter', 'cancelled'
        )
    ),
    CONSTRAINT jobs_priority_bounded CHECK (priority BETWEEN -1000 AND 1000),
    CONSTRAINT jobs_attempts_valid CHECK (
        attempt_count >= 0 AND max_attempts BETWEEN 1 AND 100 AND attempt_count <= max_attempts
    ),
    CONSTRAINT jobs_attempt_state_valid CHECK (
        (state NOT IN ('queued', 'retry_wait') OR attempt_count < max_attempts) AND
        (state NOT IN ('leased', 'running') OR attempt_count > 0)
    ),
    CONSTRAINT jobs_lease_owner_length
        CHECK (lease_owner IS NULL OR length(lease_owner) BETWEEN 1 AND 128),
    CONSTRAINT jobs_lease_token_hash_length
        CHECK (lease_token_hash IS NULL OR octet_length(lease_token_hash) = 32),
    CONSTRAINT jobs_lease_fields_consistent CHECK (
        (
            state IN ('leased', 'running') AND
            lease_owner IS NOT NULL AND
            lease_token_hash IS NOT NULL AND
            lease_expires_at IS NOT NULL AND
            heartbeat_at IS NOT NULL
        ) OR (
            state NOT IN ('leased', 'running') AND
            lease_owner IS NULL AND
            lease_token_hash IS NULL AND
            lease_expires_at IS NULL AND
            heartbeat_at IS NULL
        )
    ),
    CONSTRAINT jobs_running_started CHECK (state <> 'running' OR started_at IS NOT NULL),
    CONSTRAINT jobs_completed_at_consistent CHECK (
        (state IN ('succeeded', 'dead_letter', 'cancelled')) = (completed_at IS NOT NULL)
    ),
    CONSTRAINT jobs_last_error_class_known CHECK (
        last_error_class IS NULL OR last_error_class IN ('retryable', 'permanent', 'worker')
    ),
    CONSTRAINT jobs_last_error_code_format CHECK (
        last_error_code IS NULL OR last_error_code ~ '^[a-z][a-z0-9._-]{0,127}$'
    ),
    CONSTRAINT jobs_idempotency_unique
        UNIQUE (workspace_id, queue, kind, idempotency_key)
);

CREATE INDEX jobs_available_idx
    ON open_aspm.jobs (queue, state, available_at, priority DESC, created_at, id)
    WHERE state IN ('queued', 'retry_wait');
CREATE INDEX jobs_expired_lease_idx
    ON open_aspm.jobs (queue, lease_expires_at, workspace_id, id)
    WHERE state IN ('leased', 'running');
CREATE INDEX jobs_workspace_active_idx
    ON open_aspm.jobs (queue, workspace_id, state)
    WHERE state IN ('leased', 'running');
CREATE INDEX jobs_operation_idx
    ON open_aspm.jobs (workspace_id, operation_id)
    WHERE operation_id IS NOT NULL;

CREATE TABLE open_aspm.job_attempts (
    job_id varchar(128) NOT NULL,
    workspace_id varchar(128) NOT NULL,
    attempt_number smallint NOT NULL,
    lease_owner varchar(128) NOT NULL,
    leased_at timestamptz NOT NULL,
    started_at timestamptz,
    last_heartbeat_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome varchar(64),
    safe_error_code varchar(128),
    PRIMARY KEY (job_id, attempt_number),
    CONSTRAINT job_attempts_job_fk
        FOREIGN KEY (workspace_id, job_id)
        REFERENCES open_aspm.jobs (workspace_id, id) ON DELETE RESTRICT,
    CONSTRAINT job_attempts_number_positive CHECK (attempt_number > 0),
    CONSTRAINT job_attempts_owner_length CHECK (length(lease_owner) BETWEEN 1 AND 128),
    CONSTRAINT job_attempts_outcome_known CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'retry', 'dead_letter', 'cancelled', 'lease_expired')
    ),
    CONSTRAINT job_attempts_finished_consistent
        CHECK ((finished_at IS NULL) = (outcome IS NULL)),
    CONSTRAINT job_attempts_error_code_format CHECK (
        safe_error_code IS NULL OR safe_error_code ~ '^[a-z][a-z0-9._-]{0,127}$'
    )
);

CREATE INDEX job_attempts_workspace_idx
    ON open_aspm.job_attempts (workspace_id, job_id, attempt_number);
