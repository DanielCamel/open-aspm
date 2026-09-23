-- +goose Up
CREATE TABLE open_aspm.import_parse_outputs (
    workspace_id varchar(128) NOT NULL,
    import_id varchar(128) NOT NULL,
    raw_artifact_id varchar(128) NOT NULL,
    format_name varchar(32) NOT NULL,
    format_version varchar(32) NOT NULL,
    parser_name varchar(64) NOT NULL,
    parser_version varchar(64) NOT NULL,
    warning_count integer NOT NULL,
    warnings_truncated boolean NOT NULL,
    recorded_at timestamptz NOT NULL,
    record_fingerprint bytea NOT NULL,
    PRIMARY KEY (workspace_id, import_id, parser_name, parser_version),
    CONSTRAINT import_parse_outputs_artifact_fk
        FOREIGN KEY (workspace_id, raw_artifact_id, import_id)
        REFERENCES open_aspm.raw_artifacts (workspace_id, id, import_id),
    CONSTRAINT import_parse_outputs_format_name_length
        CHECK (length(format_name) BETWEEN 1 AND 32),
    CONSTRAINT import_parse_outputs_format_version_length
        CHECK (length(format_version) BETWEEN 1 AND 32),
    CONSTRAINT import_parse_outputs_parser_name_format
        CHECK (parser_name ~ '^[a-z][a-z0-9._-]{0,63}$'),
    CONSTRAINT import_parse_outputs_parser_version_length
        CHECK (length(parser_version) BETWEEN 1 AND 64),
    CONSTRAINT import_parse_outputs_warning_count_valid CHECK (warning_count >= 0),
    CONSTRAINT import_parse_outputs_fingerprint_length
        CHECK (octet_length(record_fingerprint) = 32)
);

CREATE TABLE open_aspm.import_parse_warnings (
    workspace_id varchar(128) NOT NULL,
    import_id varchar(128) NOT NULL,
    parser_name varchar(64) NOT NULL,
    parser_version varchar(64) NOT NULL,
    warning_index integer NOT NULL,
    code varchar(64) NOT NULL,
    source_pointer varchar(1024) NOT NULL,
    field_count integer NOT NULL,
    fields text[] NOT NULL,
    fields_truncated boolean NOT NULL,
    PRIMARY KEY (
        workspace_id, import_id, parser_name, parser_version, warning_index
    ),
    CONSTRAINT import_parse_warnings_output_fk
        FOREIGN KEY (workspace_id, import_id, parser_name, parser_version)
        REFERENCES open_aspm.import_parse_outputs (
            workspace_id, import_id, parser_name, parser_version
        ),
    CONSTRAINT import_parse_warnings_index_valid CHECK (warning_index >= 0),
    CONSTRAINT import_parse_warnings_code_format
        CHECK (code ~ '^[a-z][a-z0-9._-]{0,63}$'),
    CONSTRAINT import_parse_warnings_source_pointer_length
        CHECK (length(source_pointer) BETWEEN 0 AND 1024),
    CONSTRAINT import_parse_warnings_field_count_valid CHECK (field_count > 0),
    CONSTRAINT import_parse_warnings_fields_bounded CHECK (cardinality(fields) <= 64)
);
