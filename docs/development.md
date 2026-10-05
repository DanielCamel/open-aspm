# Development

## Prerequisites

- Go 1.26 or a newer supported Go release
- Git

PostgreSQL is required for migration integration tests and will be required by
the application runtime. With the default Go toolchain behavior, an older local
`go` command may download a compatible toolchain declared by `go.mod`.

## Build and verify

From the repository root:

```bash
make check
```

The check verifies formatting, runs `go vet`, executes the tests, and builds the
binary. Run `make test-race` before submitting concurrency-sensitive changes.
Use `make format` to apply Go formatting.

Generated binaries belong in `bin/`, which is ignored by Git.

## PostgreSQL migrations

Schema migrations are embedded in the binary and run explicitly:

```bash
export OPEN_ASPM_DATABASE_URL='postgres://open_aspm_migration:password@localhost/open_aspm?sslmode=require'
go run ./cmd/open-aspm migrate status
go run ./cmd/open-aspm migrate up
```

Use a schema-owner connection only for migration commands. The server runtime
must use a separate, less-privileged role. See
[ADR-0006](adr/0006-postgresql-access-and-migrations.md) for role and backup
requirements.

The integration test requires an isolated disposable PostgreSQL instance and an
administrative URL that may create and drop temporary databases and roles:

```bash
OPEN_ASPM_TEST_DATABASE_ADMIN_URL='postgres://postgres:postgres@localhost/postgres?sslmode=disable' \
  make test-integration
```

Never point this test at a shared or production-like database.

The current migrations add the minimal workspace, application, ingestion,
queue, Catalog, and authorization tables. After migrations, grant the runtime
role only the DML needed by these implementations (substitute the deployment's
role name):

```sql
GRANT SELECT ON open_aspm.workspaces TO open_aspm_runtime;
GRANT SELECT ON open_aspm.applications TO open_aspm_runtime;
GRANT SELECT, INSERT, UPDATE ON open_aspm.imports TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.import_create_idempotency TO open_aspm_runtime;
GRANT SELECT, INSERT, UPDATE ON open_aspm.raw_artifacts TO open_aspm_runtime;
GRANT SELECT, INSERT, UPDATE ON open_aspm.operations TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.import_complete_idempotency TO open_aspm_runtime;
GRANT SELECT, INSERT, UPDATE ON open_aspm.jobs TO open_aspm_runtime;
GRANT SELECT, INSERT, UPDATE ON open_aspm.job_attempts TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.scans TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.scan_scopes TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.observations TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.observation_locations TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.observation_fingerprints TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.observation_normalizations TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.observation_correlation_outcomes TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.import_parse_outputs TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.import_parse_warnings TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.repositories TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.repository_create_idempotency TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.application_repository_relationships TO open_aspm_runtime;
GRANT SELECT, INSERT ON open_aspm.import_analysis_contexts TO open_aspm_runtime;
GRANT SELECT ON open_aspm.principals TO open_aspm_runtime;
GRANT SELECT ON open_aspm.workspace_memberships TO open_aspm_runtime;
GRANT SELECT ON open_aspm.service_accounts TO open_aspm_runtime;
GRANT SELECT ON open_aspm.role_capabilities TO open_aspm_runtime;
GRANT SELECT ON open_aspm.role_bindings TO open_aspm_runtime;
GRANT SELECT, UPDATE (last_used_at) ON open_aspm.api_tokens TO open_aspm_runtime;
GRANT SELECT ON open_aspm.api_token_capabilities TO open_aspm_runtime;
GRANT SELECT ON open_aspm.api_token_application_scopes TO open_aspm_runtime;
```

The ingestion service implements the `createImport` reservation, streaming
upload, and `completeImport` application boundaries. It authorizes
`imports:create` or `imports:upload` before persistence, scopes mutations to the
reservation owner, and stores verified immutable evidence through BlobStore.
Upload attempts use database fencing and recover when blob publication succeeds
before the metadata transaction. Completing an uploaded import atomically
creates its queued public operation, durable idempotency result, job, and import
state transition. HTTP routing, authentication, operation queries, and worker
processing remain separate delivery slices.

The ingestion context owns immutable Scan and versioned ScanScope writes. The
findings context owns immutable Observation writes, including structured source
locations and scanner-provided fingerprints. Runtime roles intentionally have
no `UPDATE` or `DELETE` grant on those tables. Replaying the same source run or
parser-versioned source result returns the original record; conflicting reuse
is rejected. This persistence is internal until the worker and authorized query
contracts are wired.

The `import.process` application handler reauthorizes the initiating principal
and recorded system capability before resolving a BlobStore key. It compares
BlobStore metadata with committed PostgreSQL metadata, consumes the verified
stream through the bounded SARIF parser, records versioned parser warnings,
and writes Scan and Observation records only through their owning module
interfaces. Exact job replay returns the retained domain result. Malformed or
unsupported evidence produces a stable sanitized terminal failure; transient
database, authorization-backend, and BlobStore failures remain retryable until
the job's attempt or age limit is reached.

After each immutable Observation is stored, the handler applies the explicit
SARIF normalization version and records a separate immutable normalization.
Source severity remains on the Observation; normalized severity, an explicitly
unknown category, the retained source rule key, and the bounded primary
artifact location remain attributable to the normalizer version. Exact replay
does not duplicate output, and a future version may coexist without rewriting
the earlier interpretation. Successful Finding correlation and effective
severity are not implemented by this stage.

The correlation context can persist an explicit immutable `uncorrelated`
outcome for one Observation, normalization version, and correlation algorithm
version. Canonical reason codes preserve whether target, analysis, scanner,
rule, package, vulnerability, location, or source context was unknown or
unsafe. Exact replay is idempotent and conflicting reuse is rejected. After
normalization, the import worker retains `correlation-dispatch` version `1` for
Imports without accepted analysis context, preserving the unknown target and
analysis reasons across retries. An internally supplied `sast + repository`
context uses version `2` and removes only those two blockers; the current SARIF
adapter still retains unknown scanner-family and stable-source-context reasons
and creates no Finding.

The Catalog context owns workspace-scoped Repository identities and temporal
Application-to-Repository relationships. Its application service authorizes
Repository creation and linking separately, permits duplicate display names,
and enforces create idempotency and one active relationship through durable
database constraints. Runtime roles can insert and read this first slice but
cannot update or delete it; rename and relationship-end operations are not yet
implemented. Authenticated Repository creation is exposed over HTTP;
Application-to-Repository linking remains internal.

An optional immutable Import analysis context can retain the exact active
Application-to-Repository relationship, asserting principal, assertion source,
and acceptance time. It participates in createImport idempotency and every Scan
derived from the artifact references it. This is implemented at the application
and worker layers but is not accepted by the current HTTP server yet.

The authorization foundation persists principals, workspace memberships,
service-account expiry, capability-bearing roles, temporal workspace or
Application role bindings, and API-token capability and Application scopes.
Its evaluator denies unknown, inactive, expired, revoked, cross-workspace, and
out-of-scope decisions and treats token scope only as a restriction on current
role grants. The authentication service generates 256-bit token secrets,
stores only versioned HMAC verifier material, verifies credentials in constant
time, and conditionally records use while a token remains active. The HTTP
server authenticates API tokens and applies their capability scope to
Repository creation. General operator-facing identity management is not
implemented by this stage.

## Initial operator bootstrap

After applying migrations to a fresh database, configure a separately managed
HMAC verifier key and run the one-time bootstrap command. The key is a secret;
the key ID is non-secret and identifies the key version:

```bash
export OPEN_ASPM_DATABASE_URL='postgres://open_aspm_bootstrap:password@localhost/open_aspm?sslmode=require'
export OPEN_ASPM_TOKEN_VERIFIER_KEY_ID='bootstrap-v1'
export OPEN_ASPM_TOKEN_VERIFIER_KEY='<unpadded base64url encoding of at least 32 random bytes>'
open-aspm bootstrap init
```

The command atomically creates one Workspace, one Application, an initial
service-account principal, its workspace-scoped role, and a 30-day operator
token. The token plaintext is written once to standard output and is never
stored. Capture it through an appropriately protected operator channel; do not
place it in shell history, logs, source control, or ordinary support output.

`bootstrap init` refuses a database that already contains Workspace,
Application, or principal state not tracked by the bootstrap record. It also
refuses a second initialization. If the one-time token is lost or expires, use
the same database and verifier-key configuration to recover:

```bash
open-aspm bootstrap token
```

This atomically revokes every previous token for the initial operator and emits
one replacement. It does not recreate or rename domain identities. Bootstrap
requires an operator-only database role with the necessary DML and must never
cause the HTTP server to run with migration-owner credentials.

After migrations, a dedicated bootstrap role needs only the following table
privileges (substitute its deployment-specific role name):

```sql
GRANT USAGE ON SCHEMA open_aspm TO open_aspm_bootstrap;
GRANT SELECT, INSERT ON open_aspm.workspaces TO open_aspm_bootstrap;
GRANT SELECT, INSERT ON open_aspm.applications TO open_aspm_bootstrap;
GRANT SELECT, INSERT ON open_aspm.principals TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.workspace_memberships TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.service_accounts TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.roles TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.role_capabilities TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.role_bindings TO open_aspm_bootstrap;
GRANT SELECT, INSERT, UPDATE (revoked_at) ON open_aspm.api_tokens TO open_aspm_bootstrap;
GRANT INSERT ON open_aspm.api_token_capabilities TO open_aspm_bootstrap;
GRANT SELECT, INSERT ON open_aspm.bootstrap_installations TO open_aspm_bootstrap;
```

This handler is implemented and tested internally, but no `open-aspm worker`
command or deployment configuration is available yet. Runtime registration,
lease configuration, and storage-backend construction remain a separate
delivery slice.

Queue payloads contain only bounded identifiers and metadata. Raw reports and
credentials do not belong in queue rows. Lease-token plaintext is returned
only to the acquiring worker and must be redacted from logs and telemetry.

BlobStore unit tests use temporary filesystem roots. The S3 adapter conformance
suite requires a disposable S3-compatible service; see the
[BlobStore implementation guide](storage/blobstore.md) for the command and
safety requirements.

## Run locally

```bash
go run ./cmd/open-aspm version
OPEN_ASPM_DATABASE_URL='postgres://open_aspm_runtime:password@localhost/open_aspm?sslmode=require' \
OPEN_ASPM_TOKEN_VERIFIER_KEY_ID='bootstrap-v1' \
OPEN_ASPM_TOKEN_VERIFIER_KEY='<the same verifier key used for bootstrap>' \
  go run ./cmd/open-aspm server
```

The server listens on `127.0.0.1:8080` by default. Use a dedicated runtime
database role and the same verifier-key version that created active API tokens.
The verifier key is a secret and must not be passed as a command-line flag.

Check its initial health endpoints:

```bash
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
```

Create a Repository with the one-time token returned by `bootstrap init` or
`bootstrap token`:

```bash
curl --fail-with-body \
  --request POST \
  --header "Authorization: Bearer ${OPEN_ASPM_API_TOKEN}" \
  --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: repository-example-1' \
  --data '{"display_name":"example-service"}' \
  "http://127.0.0.1:8080/api/v1/workspaces/${OPEN_ASPM_WORKSPACE_ID}/repositories"
```

To use another address explicitly:

```bash
go run ./cmd/open-aspm server --listen=0.0.0.0:8080
```

Stop the process with `Ctrl+C` or `SIGTERM`. The server stops accepting new
connections and gives active requests up to ten seconds to finish. Override the
period with `--shutdown-timeout`.

## Build metadata

Release automation may set these variables with Go linker flags:

```text
github.com/spectremi/open-aspm/internal/version.Version
github.com/spectremi/open-aspm/internal/version.Commit
github.com/spectremi/open-aspm/internal/version.Date
```

Development builds intentionally report `dev`, `unknown`, and `unknown`.
