-- +goose Up
CREATE TABLE open_aspm.observation_normalizations (
    workspace_id varchar(128) NOT NULL,
    observation_id varchar(128) NOT NULL,
    normalizer_name varchar(64) NOT NULL,
    normalizer_version varchar(64) NOT NULL,
    category varchar(32) NOT NULL,
    severity varchar(32) NOT NULL,
    rule_kind varchar(32) NOT NULL,
    rule_id varchar(512),
    location_kind varchar(32) NOT NULL,
    location_uri text,
    location_uri_base_id varchar(512),
    location_start_line integer,
    location_start_column integer,
    location_end_line integer,
    location_end_column integer,
    normalized_at timestamptz NOT NULL,
    record_fingerprint bytea NOT NULL,
    PRIMARY KEY (
        workspace_id, observation_id, normalizer_name, normalizer_version
    ),
    CONSTRAINT observation_normalizations_observation_fk
        FOREIGN KEY (workspace_id, observation_id)
        REFERENCES open_aspm.observations (workspace_id, id),
    CONSTRAINT observation_normalizations_name_format CHECK (
        normalizer_name ~ '^[a-z][a-z0-9._-]{0,63}$'
    ),
    CONSTRAINT observation_normalizations_version_length CHECK (
        length(normalizer_version) BETWEEN 1 AND 64
    ),
    CONSTRAINT observation_normalizations_category_known CHECK (
        category = 'unknown'
    ),
    CONSTRAINT observation_normalizations_severity_known CHECK (
        severity IN (
            'unknown', 'not_applicable', 'informational',
            'low', 'medium', 'high', 'critical'
        )
    ),
    CONSTRAINT observation_normalizations_rule_kind_known CHECK (
        rule_kind IN ('unknown', 'source')
    ),
    CONSTRAINT observation_normalizations_rule_consistent CHECK (
        (rule_kind = 'unknown' AND rule_id IS NULL) OR
        (rule_kind = 'source' AND rule_id IS NOT NULL)
    ),
    CONSTRAINT observation_normalizations_rule_id_length CHECK (
        rule_id IS NULL OR length(rule_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT observation_normalizations_location_kind_known CHECK (
        location_kind IN ('unknown', 'artifact')
    ),
    CONSTRAINT observation_normalizations_location_consistent CHECK (
        (
            location_kind = 'unknown' AND location_uri IS NULL AND
            location_uri_base_id IS NULL AND location_start_line IS NULL AND
            location_start_column IS NULL AND location_end_line IS NULL AND
            location_end_column IS NULL
        ) OR (
            location_kind = 'artifact' AND location_uri IS NOT NULL
        )
    ),
    CONSTRAINT observation_normalizations_location_uri_bounded CHECK (
        location_uri IS NULL OR octet_length(location_uri) BETWEEN 1 AND 1048576
    ),
    CONSTRAINT observation_normalizations_location_uri_base_id_length CHECK (
        location_uri_base_id IS NULL OR
        length(location_uri_base_id) BETWEEN 1 AND 512
    ),
    CONSTRAINT observation_normalizations_location_region_valid CHECK (
        (location_start_line IS NULL OR location_start_line > 0) AND
        (location_start_column IS NULL OR location_start_column > 0) AND
        (location_end_line IS NULL OR location_end_line > 0) AND
        (location_end_column IS NULL OR location_end_column > 0)
    ),
    CONSTRAINT observation_normalizations_location_region_order_valid CHECK (
        location_start_line IS NULL OR location_end_line IS NULL OR
        location_end_line > location_start_line OR
        (
            location_end_line = location_start_line AND
            (
                location_start_column IS NULL OR location_end_column IS NULL OR
                location_end_column >= location_start_column
            )
        )
    ),
    CONSTRAINT observation_normalizations_fingerprint_length CHECK (
        octet_length(record_fingerprint) = 32
    )
);
