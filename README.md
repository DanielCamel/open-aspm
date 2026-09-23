# Open ASPM

Open-source Application Security Posture Management platform for aggregating,
correlating, prioritizing, and tracking security findings across the SDLC.

> [!WARNING]
> Open ASPM is in pre-alpha development and is not ready for production use.
> Interfaces, data models, and deployment procedures may change without notice.

## Why Open ASPM?

Application security findings are fragmented across SAST, SCA, DAST, secret
scanning, container, cloud, and infrastructure security tools. Open ASPM aims
to provide a common place to correlate, prioritize, assign, and track those
findings without replacing the scanners that discover them.

## Planned capabilities

- Scanner, source control, and CI/CD integrations
- Application, repository, artifact, and environment inventory
- Finding normalization, correlation, and deduplication
- Risk-based prioritization and ownership
- Policy and Security Gate evaluation
- Suppression, exception, and risk acceptance workflows
- Audit trail, reporting, API, and CLI

## Non-goals

- Replacing source security scanners
- Automatically changing application code without review
- Claiming compliance based solely on imported findings
- Hiding scanner evidence or silently changing source severity

## Project status

Open ASPM is being built as a sequence of tested internal vertical slices.
Implementation in the repository does not necessarily mean that a capability
is reachable through the current command line or HTTP server.

Implemented and tested internally:

- explicit PostgreSQL migrations and a lease-fenced, at-least-once job queue;
- filesystem and S3-compatible immutable BlobStore adapters;
- application services for idempotent import reservation, bounded evidence
  upload, and atomic `completeImport` queueing;
- public asynchronous-operation persistence kept separate from internal queue
  lease state; and
- a bounded, deterministic, versioned SARIF 2.1.0 parser; and
- replay-safe immutable Scan and Observation persistence, including explicit
  unknown scan result, completeness, and scope values; and
- an idempotent `import.process` application handler that reauthorizes queued
  work, verifies BlobStore evidence, records parser diagnostics, and persists
  SARIF Scan and Observation output with terminal Import/Operation state.

Available to an operator today:

- `open-aspm version`;
- `open-aspm migrate status` and `open-aspm migrate up`; and
- the loopback HTTP server with `/health/live` and `/health/ready` endpoints.

Not yet available as an end-to-end user workflow:

- authenticated ingestion HTTP routes from the published OpenAPI contract;
- runtime registration and an operator command for the import worker;
- normalization, correlation, and durable Finding state;
- authorized ingestion and finding query APIs; and
- the web interface.

The architecture and domain model remain under active development. The initial
direction is tracked in [ROADMAP.md](ROADMAP.md); observable behavior and tests,
not roadmap text alone, determine whether a capability is complete.

## Development preview

Open ASPM currently provides operator commands and a minimal pre-alpha HTTP
service. The HTTP service exposes health endpoints only; the implemented
ingestion services and import processing handler are not yet wired into public
routes or a runnable worker command, and no authenticated production API is
available.

```bash
go run ./cmd/open-aspm version
go run ./cmd/open-aspm server
```

See the [development guide](docs/development.md) for prerequisites, verification
commands, and server options.

## Documentation

Architecture, threat model, integration contracts, and API documentation will
be versioned in the repository alongside the implementation.

- [Architecture overview](docs/architecture/overview.md)
- [Domain glossary](docs/glossary.md)
- [Architecture decisions](docs/adr/README.md)
- [System threat model](docs/threat-model/system.md)
- [Test data policy](docs/testing/test-data-policy.md)
- [Ingestion API v1 contract](docs/api/ingestion-v1.md)
- [SARIF parser behavior](docs/parsing/sarif.md)
- [Development guide](docs/development.md)
- [Contributor tasks](CONTRIBUTOR_TASKS.md)

## Contributing

Contributions and design feedback are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) and [GOVERNANCE.md](GOVERNANCE.md) before
opening a pull request. By participating, you agree to follow our
[Code of Conduct](CODE_OF_CONDUCT.md).

New contributors can select a bounded task from
[CONTRIBUTOR_TASKS.md](CONTRIBUTOR_TASKS.md).

## Security

Do not disclose suspected vulnerabilities in public issues. Follow the private
reporting instructions in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
