-- +goose Up
CREATE TABLE open_aspm.jobs (
    id text PRIMARY KEY,
    workspace_id text NOT NULL,
    operation_id text,
    queue text NOT NULL,
    kind text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    payload jsonb NOT NULL,
    idempotency_key text NOT NULL,
    state text NOT NULL DEFAULT 'queued' CHECK (
        state IN ('queued', 'leased', 'running', 'retry_wait', 'succeeded', 'dead_letter', 'cancelled')
    ),
    priority integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    lease_owner text,
    lease_token text,
    lease_expires_at timestamptz,
    heartbeat_at timestamptz,
    cancellation_requested_at timestamptz,
    last_error_class text,
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    completed_at timestamptz,
    CONSTRAINT jobs_idempotency_unique UNIQUE (
        workspace_id,
        queue,
        kind,
        idempotency_key
    ),
    CONSTRAINT jobs_lease_fields_consistent CHECK (
        (state IN ('leased', 'running') AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (state NOT IN ('leased', 'running') AND lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX jobs_available_idx
    ON open_aspm.jobs (queue, priority DESC, available_at, created_at)
    WHERE state IN ('queued', 'retry_wait');

CREATE INDEX jobs_expired_lease_idx
    ON open_aspm.jobs (lease_expires_at)
    WHERE state IN ('leased', 'running');

CREATE INDEX jobs_workspace_active_idx
    ON open_aspm.jobs (workspace_id, queue)
    WHERE state IN ('leased', 'running');

CREATE TABLE open_aspm.job_attempts (
    job_id text NOT NULL REFERENCES open_aspm.jobs (id) ON DELETE RESTRICT,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    lease_owner text NOT NULL,
    started_at timestamptz,
    last_heartbeat_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text CHECK (
        outcome IS NULL OR outcome IN ('succeeded', 'retry', 'dead_letter', 'cancelled', 'lease_expired')
    ),
    safe_error_code text,
    PRIMARY KEY (job_id, attempt_number)
);

-- +goose Down
DROP TABLE open_aspm.job_attempts;
DROP TABLE open_aspm.jobs;
