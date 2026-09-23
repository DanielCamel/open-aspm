-- +goose Up
CREATE TABLE open_aspm.applications (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, id),
    CONSTRAINT applications_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES open_aspm.workspaces (id),
    CONSTRAINT applications_id_length CHECK (length(id) BETWEEN 3 AND 128)
);

CREATE TABLE open_aspm.imports (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    application_id varchar(128) NOT NULL,
    created_by_principal_id varchar(128) NOT NULL,
    state varchar(32) NOT NULL,
    report_format_name varchar(32) NOT NULL,
    report_format_version varchar(32),
    original_filename varchar(255),
    expected_size_bytes bigint,
    expected_sha256 bytea,
    max_bytes bigint NOT NULL,
    upload_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (workspace_id, id),
    CONSTRAINT imports_application_fk
        FOREIGN KEY (workspace_id, application_id)
        REFERENCES open_aspm.applications (workspace_id, id),
    CONSTRAINT imports_id_length CHECK (length(id) BETWEEN 3 AND 128),
    CONSTRAINT imports_principal_id_length
        CHECK (length(created_by_principal_id) BETWEEN 3 AND 128),
    CONSTRAINT imports_state_known CHECK (
        state IN (
            'awaiting_upload', 'uploading', 'uploaded', 'queued',
            'processing', 'succeeded', 'failed', 'rejected', 'abandoned'
        )
    ),
    CONSTRAINT imports_report_format_name_length
        CHECK (length(report_format_name) BETWEEN 1 AND 32),
    CONSTRAINT imports_report_format_version_length CHECK (
        report_format_version IS NULL OR length(report_format_version) BETWEEN 1 AND 32
    ),
    CONSTRAINT imports_original_filename_length CHECK (
        original_filename IS NULL OR length(original_filename) BETWEEN 1 AND 255
    ),
    CONSTRAINT imports_expected_size_valid
        CHECK (expected_size_bytes IS NULL OR expected_size_bytes >= 0),
    CONSTRAINT imports_expected_sha256_length
        CHECK (expected_sha256 IS NULL OR octet_length(expected_sha256) = 32),
    CONSTRAINT imports_max_bytes_positive CHECK (max_bytes > 0),
    CONSTRAINT imports_expected_size_within_limit CHECK (
        expected_size_bytes IS NULL OR expected_size_bytes <= max_bytes
    ),
    CONSTRAINT imports_upload_expiry_valid CHECK (upload_expires_at > created_at),
    CONSTRAINT imports_updated_at_valid CHECK (updated_at >= created_at)
);

CREATE INDEX imports_creator_idx
    ON open_aspm.imports (workspace_id, created_by_principal_id, id);

CREATE TABLE open_aspm.import_create_idempotency (
    workspace_id varchar(128) NOT NULL,
    principal_id varchar(128) NOT NULL,
    api_major_version smallint NOT NULL,
    operation varchar(64) NOT NULL,
    idempotency_key varchar(128) NOT NULL,
    request_fingerprint bytea NOT NULL,
    import_id varchar(128) NOT NULL,
    completed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (
        workspace_id, principal_id, api_major_version, operation, idempotency_key
    ),
    CONSTRAINT import_create_idempotency_workspace_fk
        FOREIGN KEY (workspace_id) REFERENCES open_aspm.workspaces (id),
    CONSTRAINT import_create_idempotency_import_fk
        FOREIGN KEY (workspace_id, import_id)
        REFERENCES open_aspm.imports (workspace_id, id)
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT import_create_idempotency_principal_length
        CHECK (length(principal_id) BETWEEN 3 AND 128),
    CONSTRAINT import_create_idempotency_api_version_positive
        CHECK (api_major_version > 0),
    CONSTRAINT import_create_idempotency_operation_format
        CHECK (operation ~ '^[a-z][a-z0-9._:-]{0,63}$'),
    CONSTRAINT import_create_idempotency_key_format
        CHECK (idempotency_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    CONSTRAINT import_create_idempotency_fingerprint_length
        CHECK (octet_length(request_fingerprint) = 32),
    CONSTRAINT import_create_idempotency_retention CHECK (
        expires_at >= completed_at + interval '24 hours'
    )
);
