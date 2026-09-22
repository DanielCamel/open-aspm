# ADR-0006: PostgreSQL access and schema migrations

- Status: Accepted
- Date: 2026-09-22
- Owners: Open ASPM maintainers
- Related: issue #15, ADR-0003

## Context

Open ASPM needs repeatable PostgreSQL schema evolution before domain tables are
introduced. Migrations may be started by more than one deployment instance, and
the application runtime must not retain schema-owner privileges. Database URLs
are secrets and must not be accepted as command-line flags or written to logs.

## Decision

- Use `pgx/v5` as the PostgreSQL driver.
- Use the `goose/v3` Provider API with migrations embedded in the binary.
- Use PostgreSQL session advisory locking so only one migration runner applies
  migrations at a time.
- Run migrations explicitly with `open-aspm migrate up`; the HTTP server never
  changes the schema during startup.
- Read the connection URL from `OPEN_ASPM_DATABASE_URL`.
- Keep migrations sequential, immutable after release, and transactional unless
  a reviewed migration explicitly declares otherwise.
- Use separate roles:
  - a migration role owns the database objects and can change the schema;
  - a runtime role receives only `CONNECT`, schema `USAGE`, and the exact table
    and sequence privileges needed by the application.
- Production rollbacks use restore or a reviewed forward migration. Destructive
  `down` migration commands are intentionally not exposed by the binary.

The first migration creates only the `open_aspm` schema. Domain tables remain
deferred until their contracts are accepted.

## Operations and backup expectations

Before a production migration, operators must create and verify a restorable
backup appropriate to their PostgreSQL deployment, review release notes for
locking or rewrite risks, and ensure that the migration role is available. A
failed transactional migration is not recorded as applied; `migrate status`
continues to report it as pending. Operators must diagnose the failure before
retrying and must never edit an already released migration.

Example role setup (names are deployment-specific):

```sql
CREATE ROLE open_aspm_migration LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
CREATE DATABASE open_aspm OWNER open_aspm_migration;
CREATE ROLE open_aspm_runtime LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT;
GRANT CONNECT ON DATABASE open_aspm TO open_aspm_runtime;
-- Run migrations as open_aspm_migration, then grant runtime access explicitly:
GRANT USAGE ON SCHEMA open_aspm TO open_aspm_runtime;
```

Future table migrations must include narrowly scoped runtime grants or document
the deployment grant step. `PUBLIC` never receives object-creation privileges in
the application schema.

## Verification

CI starts an isolated PostgreSQL service and creates temporary migration and
runtime roles with `NOSUPERUSER`. It verifies first-run and idempotent migration,
version reporting, rollback of a deliberately failed migration, and denial of
DDL to the runtime role. The test database and roles are destroyed afterward.

## Consequences

- The release artifact and migration set cannot drift.
- Concurrent deploys serialize safely.
- Compromise of the normal application connection does not grant schema DDL.
- Operators must run an explicit pre-deployment step and manage two role
  credentials.
- PostgreSQL remains a deliberate system dependency.

## Alternatives considered

- Hand-written migration runner: rejected because version tracking, ordering,
  transactions, and locking are security-sensitive infrastructure.
- Automatic migration on server startup: rejected because it gives every server
  instance schema-owner credentials and couples availability to DDL.
- ORM-managed schema changes: rejected because Open ASPM has no ORM contract and
  explicit SQL is easier to review and operate.
