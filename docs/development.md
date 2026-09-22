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

## Run locally

```bash
go run ./cmd/open-aspm version
go run ./cmd/open-aspm server
```

The server listens on `127.0.0.1:8080` by default. This loopback default avoids
accidentally exposing an unauthenticated pre-alpha service.

Check its initial health endpoints:

```bash
curl --fail http://127.0.0.1:8080/health/live
curl --fail http://127.0.0.1:8080/health/ready
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
