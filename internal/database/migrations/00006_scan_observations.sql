-- +goose Up
-- Imports and raw artifacts already have workspace-scoped primary keys. These
-- wider keys let the database also prove that a scan's application and raw
-- artifact belong to the same import.
ALTER TABLE open_aspm.imports
    ADD CONSTRAINT imports_workspace_id_application_unique
    UNIQUE (workspace_id, id, application_id);

ALTER TABLE open_aspm.raw_artifacts
    ADD CONSTRAINT raw_artifacts_workspace_id_import_unique
    UNIQUE (workspace_id, id, import_id);

CREATE TABLE open_aspm.scans (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    import_id varchar(128) NOT NULL,
    application_id varchar(128) NOT NULL,
    raw_artifact_id varchar(128) NOT NULL,
    source_run_index integer NOT NULL,
    result varchar(32) NOT NULL,
    completeness varchar(32) NOT NULL,
    scanner_name varchar(255) NOT NULL,
    scanner_full_name varchar(512),
    scanner_version varchar(128),
    scanner_semantic_version varchar(128),
    automation_id varchar(512),
    automation_guid varchar(128),
    automation_correlation_guid varchar(128),
    parser_name varchar(64) NOT NULL,
    parser_version varchar(64) NOT NULL,
    source_pointer varchar(1024) NOT NULL,
    source_started_at timestamptz,
    source_ended_at timestamptz,
    received_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    record_fingerprint bytea NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, import_id, source_run_index),
    UNIQUE (workspace_id, id, application_id),
    UNIQUE (workspace_id, id, raw_artifact_id),
    CONSTRAINT scans_import_fk
        FOREIGN KEY (workspace_id, import_id, application_id)
        REFERENCES open_aspm.imports (workspace_id, id, application_id),
    CONSTRAINT scans_raw_artifact_fk
        FOREIGN KEY (workspace_id, raw_artifact_id, import_id)
        REFERENCES open_aspm.raw_artifacts (workspace_id, id, import_id),
    CONSTRAINT scans_id_length CHECK (length(id) BETWEEN 3 AND 128),
    CONSTRAINT scans_source_run_index_valid CHECK (source_run_index >= 0),
    CONSTRAINT scans_result_known CHECK (
        result IN ('succeeded', 'failed', 'cancelled', 'timed_out', 'unknown')
    ),
    CONSTRAINT scans_completeness_known CHECK (
        completeness IN ('full', 'partial', 'incremental', 'unknown')
    ),
    CONSTRAINT scans_scanner_name_length CHECK (length(scanner_name) BETWEEN 1 AND 255),
    CONSTRAINT scans_scanner_full_name_length CHECK (
        scanner_full_name IS NULL OR length(scanner_full_name) BETWEEN 1 AND 512
    ),
    CONSTRAINT scans_scanner_version_length CHECK (
        scanner_version IS NULL OR length(scanner_version) BETWEEN 1 AND 128
    ),
    CONSTRAINT scans_scanner_semantic_version_length CHECK (
        scanner_semantic_version IS NULL OR length(scanner_semantic_version) BETWEEN 1 AND 128
    ),
    CONSTRAINT scans_automation_id_length CHECK (
        automation_id IS NULL OR length(automation_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT scans_automation_guid_length CHECK (
        automation_guid IS NULL OR length(automation_guid) BETWEEN 1 AND 128
    ),
    CONSTRAINT scans_automation_correlation_guid_length CHECK (
        automation_correlation_guid IS NULL OR length(automation_correlation_guid) BETWEEN 1 AND 128
    ),
    CONSTRAINT scans_parser_name_format CHECK (
        parser_name ~ '^[a-z][a-z0-9._-]{0,63}$'
    ),
    CONSTRAINT scans_parser_version_length CHECK (length(parser_version) BETWEEN 1 AND 64),
    CONSTRAINT scans_source_pointer_length CHECK (length(source_pointer) BETWEEN 1 AND 1024),
    CONSTRAINT scans_source_times_valid CHECK (
        source_started_at IS NULL OR source_ended_at IS NULL OR source_ended_at >= source_started_at
    ),
    CONSTRAINT scans_recorded_at_valid CHECK (recorded_at >= received_at),
    CONSTRAINT scans_record_fingerprint_length CHECK (octet_length(record_fingerprint) = 32)
);

CREATE INDEX scans_application_idx
    ON open_aspm.scans (workspace_id, application_id, recorded_at, id);

CREATE TABLE open_aspm.scan_scopes (
    workspace_id varchar(128) NOT NULL,
    scan_id varchar(128) NOT NULL,
    application_id varchar(128) NOT NULL,
    schema_version smallint NOT NULL,
    scanner_family varchar(128) NOT NULL,
    scanner_instance_id varchar(128),
    scanner_configuration_id varchar(128),
    scanner_configuration_hash varchar(128),
    analysis_kind varchar(64) NOT NULL,
    coverage_metadata jsonb NOT NULL,
    scope_fingerprint bytea NOT NULL,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (workspace_id, scan_id),
    CONSTRAINT scan_scopes_scan_fk
        FOREIGN KEY (workspace_id, scan_id, application_id)
        REFERENCES open_aspm.scans (workspace_id, id, application_id),
    CONSTRAINT scan_scopes_schema_version_positive CHECK (schema_version > 0),
    CONSTRAINT scan_scopes_scanner_family_format CHECK (
        scanner_family ~ '^[a-z][a-z0-9._-]{0,127}$'
    ),
    CONSTRAINT scan_scopes_scanner_instance_id_length CHECK (
        scanner_instance_id IS NULL OR length(scanner_instance_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT scan_scopes_scanner_configuration_id_length CHECK (
        scanner_configuration_id IS NULL OR length(scanner_configuration_id) BETWEEN 1 AND 128
    ),
    CONSTRAINT scan_scopes_scanner_configuration_hash_length CHECK (
        scanner_configuration_hash IS NULL OR length(scanner_configuration_hash) BETWEEN 1 AND 128
    ),
    CONSTRAINT scan_scopes_analysis_kind_format CHECK (
        analysis_kind ~ '^[a-z][a-z0-9._-]{0,63}$'
    ),
    CONSTRAINT scan_scopes_coverage_object CHECK (jsonb_typeof(coverage_metadata) = 'object'),
    CONSTRAINT scan_scopes_coverage_bounded CHECK (pg_column_size(coverage_metadata) <= 65536),
    CONSTRAINT scan_scopes_fingerprint_length CHECK (octet_length(scope_fingerprint) = 32)
);

CREATE TABLE open_aspm.observations (
    workspace_id varchar(128) NOT NULL,
    id varchar(128) NOT NULL,
    scan_id varchar(128) NOT NULL,
    raw_artifact_id varchar(128) NOT NULL,
    source_result_index integer NOT NULL,
    parser_name varchar(64) NOT NULL,
    parser_version varchar(64) NOT NULL,
    source_pointer varchar(1024) NOT NULL,
    source_guid varchar(128),
    source_correlation_guid varchar(128),
    source_rule_id varchar(512),
    source_rule_index integer,
    source_level varchar(64),
    source_kind varchar(64),
    source_baseline_state varchar(64),
    message_id varchar(512),
    message_text text,
    message_markdown text,
    message_arguments text[] NOT NULL,
    observed_at timestamptz,
    received_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    record_fingerprint bytea NOT NULL,
    PRIMARY KEY (workspace_id, id),
    UNIQUE (workspace_id, scan_id, parser_name, parser_version, source_result_index),
    CONSTRAINT observations_scan_fk
        FOREIGN KEY (workspace_id, scan_id, raw_artifact_id)
        REFERENCES open_aspm.scans (workspace_id, id, raw_artifact_id),
    CONSTRAINT observations_id_length CHECK (length(id) BETWEEN 3 AND 128),
    CONSTRAINT observations_source_result_index_valid CHECK (source_result_index >= 0),
    CONSTRAINT observations_parser_name_format CHECK (
        parser_name ~ '^[a-z][a-z0-9._-]{0,63}$'
    ),
    CONSTRAINT observations_parser_version_length CHECK (length(parser_version) BETWEEN 1 AND 64),
    CONSTRAINT observations_source_pointer_length CHECK (length(source_pointer) BETWEEN 1 AND 1024),
    CONSTRAINT observations_source_guid_length CHECK (
        source_guid IS NULL OR length(source_guid) BETWEEN 1 AND 128
    ),
    CONSTRAINT observations_source_correlation_guid_length CHECK (
        source_correlation_guid IS NULL OR length(source_correlation_guid) BETWEEN 1 AND 128
    ),
    CONSTRAINT observations_source_rule_id_length CHECK (
        source_rule_id IS NULL OR length(source_rule_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT observations_source_rule_index_valid CHECK (
        source_rule_index IS NULL OR source_rule_index >= 0
    ),
    CONSTRAINT observations_source_level_length CHECK (
        source_level IS NULL OR length(source_level) BETWEEN 1 AND 64
    ),
    CONSTRAINT observations_source_kind_length CHECK (
        source_kind IS NULL OR length(source_kind) BETWEEN 1 AND 64
    ),
    CONSTRAINT observations_source_baseline_state_length CHECK (
        source_baseline_state IS NULL OR length(source_baseline_state) BETWEEN 1 AND 64
    ),
    CONSTRAINT observations_message_id_length CHECK (
        message_id IS NULL OR length(message_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT observations_message_text_bounded CHECK (
        message_text IS NULL OR octet_length(message_text) <= 1048576
    ),
    CONSTRAINT observations_message_markdown_bounded CHECK (
        message_markdown IS NULL OR octet_length(message_markdown) <= 1048576
    ),
    CONSTRAINT observations_message_arguments_bounded CHECK (
        cardinality(message_arguments) <= 1000
    ),
    CONSTRAINT observations_recorded_at_valid CHECK (recorded_at >= received_at),
    CONSTRAINT observations_record_fingerprint_length CHECK (octet_length(record_fingerprint) = 32)
);

CREATE INDEX observations_scan_idx
    ON open_aspm.observations (workspace_id, scan_id, source_result_index, id);

CREATE TABLE open_aspm.observation_locations (
    workspace_id varchar(128) NOT NULL,
    observation_id varchar(128) NOT NULL,
    ordinal integer NOT NULL,
    source_pointer varchar(1024) NOT NULL,
    uri text,
    uri_base_id varchar(512),
    artifact_index integer,
    start_line integer,
    start_column integer,
    end_line integer,
    end_column integer,
    PRIMARY KEY (workspace_id, observation_id, ordinal),
    CONSTRAINT observation_locations_observation_fk
        FOREIGN KEY (workspace_id, observation_id)
        REFERENCES open_aspm.observations (workspace_id, id),
    CONSTRAINT observation_locations_ordinal_valid CHECK (ordinal >= 0),
    CONSTRAINT observation_locations_source_pointer_length CHECK (
        length(source_pointer) BETWEEN 1 AND 1024
    ),
    CONSTRAINT observation_locations_uri_bounded CHECK (
        uri IS NULL OR octet_length(uri) BETWEEN 1 AND 1048576
    ),
    CONSTRAINT observation_locations_uri_base_id_length CHECK (
        uri_base_id IS NULL OR length(uri_base_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT observation_locations_artifact_index_valid CHECK (
        artifact_index IS NULL OR artifact_index >= 0
    ),
    CONSTRAINT observation_locations_region_valid CHECK (
        (start_line IS NULL OR start_line > 0) AND
        (start_column IS NULL OR start_column > 0) AND
        (end_line IS NULL OR end_line > 0) AND
        (end_column IS NULL OR end_column > 0)
    )
);

CREATE TABLE open_aspm.observation_fingerprints (
    workspace_id varchar(128) NOT NULL,
    observation_id varchar(128) NOT NULL,
    kind varchar(32) NOT NULL,
    name varchar(512) NOT NULL,
    value text NOT NULL,
    PRIMARY KEY (workspace_id, observation_id, kind, name),
    CONSTRAINT observation_fingerprints_observation_fk
        FOREIGN KEY (workspace_id, observation_id)
        REFERENCES open_aspm.observations (workspace_id, id),
    CONSTRAINT observation_fingerprints_kind_known CHECK (kind IN ('complete', 'partial')),
    CONSTRAINT observation_fingerprints_name_length CHECK (length(name) BETWEEN 1 AND 512),
    CONSTRAINT observation_fingerprints_value_bounded CHECK (
        octet_length(value) BETWEEN 1 AND 1048576
    )
);
