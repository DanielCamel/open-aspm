-- +goose Up
CREATE SCHEMA open_aspm;
REVOKE CREATE ON SCHEMA open_aspm FROM PUBLIC;

-- +goose Down
DROP SCHEMA open_aspm;
