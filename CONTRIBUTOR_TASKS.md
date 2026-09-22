# Contributor Tasks

Open ASPM is in its architecture and foundation stage. This document lists
bounded work that new contributors can take on without relying on an
undocumented product direction.

The list is a planning aid, not a substitute for GitHub issues. Before starting
implementation, open or claim an issue and agree on scope with a maintainer.
This avoids duplicated work and prevents an early implementation from silently
locking an unstable public contract.

## How to choose a task

- Start with a task marked **Ready now**.
- Comment on or open the corresponding issue before writing substantial code.
- Keep one task per pull request.
- Include tests and documentation in the same pull request.
- Use only synthetic or openly licensed test data.
- Never commit customer reports, credentials, private scanner output, or copied
  vulnerability data with incompatible licensing.

Task sizes are estimates:

- **S**: a focused change suitable for a first contribution;
- **M**: several related changes requiring design discussion;
- **L**: a milestone that should be split into multiple issues.

## Ready now: documentation and project foundations

### DOC-001: Create the project glossary

**Size:** S
**Suggested label:** `good first issue`, `documentation`
**Depends on:** nothing

Define the initial meaning of application, component, repository, artifact,
deployment, integration, import, scan, scan scope, observation, finding,
exception, policy, and gate evaluation.

**Acceptance criteria**

- Add `docs/glossary.md`.
- Define each term without circular references.
- Include one short example where observation and finding differ.
- Link the glossary from the architecture document.

### DOC-002: Add an ADR template

**Size:** S
**Suggested label:** `good first issue`, `documentation`, `architecture`
**Depends on:** nothing

Create a lightweight Architecture Decision Record template.

**Acceptance criteria**

- Add `docs/adr/README.md` describing ADR numbering and lifecycle.
- Add `docs/adr/template.md` with context, decision, alternatives,
  consequences, security impact, and status sections.
- Document how a superseded ADR links to its replacement.

### DOC-003: Document local development prerequisites

**Size:** S
**Suggested label:** `good first issue`, `documentation`
**Depends on:** the initial Go module and development stack being selected

Create a reproducible setup guide for Linux and macOS. Do not describe commands
that do not yet work in the repository.

**Acceptance criteria**

- List supported tool versions.
- Include build, test, lint, and local service commands.
- Include a clean-environment verification procedure.

### SEC-001: Draft the system threat model

**Size:** M
**Suggested label:** `security`, `architecture`, `help wanted`
**Depends on:** architecture overview

Model the API, browser, worker, database, object storage, external connectors,
CI clients, and outbound agent as separate trust zones.

**Acceptance criteria**

- Add `docs/threat-model/system.md`.
- Identify protected assets, actors, entry points, and trust boundaries.
- Cover malicious reports, SSRF, credential theft, parser denial of service,
  cross-workspace access, webhook forgery, and compromised agents.
- Record mitigations, residual risks, and deferred controls.
- Do not claim that an unimplemented control already exists.

### SEC-002: Define secure test-data rules

**Size:** S
**Suggested label:** `good first issue`, `security`, `testing`
**Depends on:** nothing

Specify how report fixtures may be contributed and reviewed.

**Acceptance criteria**

- Document allowed licenses and required attribution.
- Require synthetic secrets and synthetic repository identifiers.
- Provide a checklist for removing personal and customer data.
- Define maximum fixture sizes for ordinary unit tests.

## Ready now: architecture decisions

These tasks produce decisions and contracts, not production implementations.

### ADR-001: Define scan scope and reconciliation semantics

**Size:** M
**Suggested label:** `architecture`, `domain-model`, `help wanted`
**Depends on:** project glossary

Specify when a missing observation may transition an existing finding to
absent and how full, partial, incremental, failed, and cancelled scans differ.

**Acceptance criteria**

- Define scan-scope identity and compatibility.
- Cover branches, commits, artifacts, scanner configuration, and monorepos.
- Include transition examples for present, absent, and reopened findings.
- State which events are immutable and which projections may change.

### ADR-002: Define asset identity and alias history

**Size:** M
**Suggested label:** `architecture`, `domain-model`, `help wanted`
**Depends on:** project glossary

Design stable identity for applications, repositories, components, artifacts,
and deployments.

**Acceptance criteria**

- Cover repository rename, transfer, fork, and monorepo cases.
- Distinguish mutable tags from immutable artifact digests.
- Define provider external IDs and historical aliases.
- Include uniqueness and workspace-isolation invariants.

### ADR-003: Define finding identity and fingerprint versioning

**Size:** M
**Suggested label:** `architecture`, `correlation`, `help wanted`
**Depends on:** ADR-001 and ADR-002

Define deterministic first-version fingerprints for SAST, SCA, container,
DAST, and secret observations.

**Acceptance criteria**

- Separate source identity from Open ASPM identity.
- Include algorithm names and versions.
- Define collision handling and alias migration.
- Explain why a parser upgrade must not silently merge findings.

### ADR-004: Choose the initial authorization model

**Size:** M
**Suggested label:** `architecture`, `security`, `identity`
**Depends on:** threat model

Define workspace membership, application access, service accounts, and scoped
API tokens.

**Acceptance criteria**

- Provide a role-to-capability matrix.
- Cover integration credentials, exceptions, policies, and audit access.
- Define authorization enforcement points.
- Document whether and where PostgreSQL Row Level Security is used.

### ADR-005: Define job leasing and retry semantics

**Size:** M
**Suggested label:** `architecture`, `backend`, `reliability`
**Depends on:** nothing

Specify the PostgreSQL-backed job lifecycle.

**Acceptance criteria**

- Define lease acquisition, heartbeat, expiry, and recovery.
- Define retry classes, backoff, cancellation, and dead-letter handling.
- Define idempotency requirements for handlers.
- Include behavior for worker termination during every state transition.

### ADR-006: Define raw artifact storage and retention

**Size:** M
**Suggested label:** `architecture`, `storage`, `security`
**Depends on:** threat model

Define the `BlobStore` contract for filesystem development storage and
S3-compatible production storage.

**Acceptance criteria**

- Cover streaming upload and download.
- Define hashes, object identity, encryption expectations, and size limits.
- Define retention, deletion, legal hold, and backup consistency.
- Prevent object keys supplied by clients from becoming filesystem paths.

## Ready after the first ADRs: implementation foundations

### DEV-001: Bootstrap the Go module and service command

**Size:** S
**Suggested label:** `good first issue`, `backend`
**Depends on:** agreement on supported Go version

Create the smallest compilable service with no domain behavior.

**Acceptance criteria**

- Add the Go module and `cmd/open-aspm` entry point.
- Support `version` and `server` commands.
- Implement graceful shutdown and structured startup errors.
- Add unit tests and documented build commands.
- Do not add a large application framework without an ADR.

### DEV-002: Add repository quality checks

**Size:** S
**Suggested label:** `good first issue`, `ci`, `testing`
**Depends on:** DEV-001

Add deterministic checks suitable for local development and GitHub Actions.

**Acceptance criteria**

- Provide one local command for formatting, tests, and static analysis.
- Add a GitHub Actions workflow with least-privilege permissions.
- Pin third-party actions to immutable commit SHAs.
- Use dependency caching without caching credentials or build output containing
  secrets.

### DEV-003: Add PostgreSQL migration infrastructure

**Size:** M
**Suggested label:** `backend`, `database`
**Depends on:** DEV-001 and an accepted database ADR

Add forward-only migrations and integration-test support.

**Acceptance criteria**

- Create and migrate a temporary test database.
- Detect schema version and failed migrations.
- Document backup expectations before production migrations.
- Do not require a PostgreSQL superuser at runtime.

### DEV-004: Implement the PostgreSQL job queue

**Size:** M
**Suggested label:** `backend`, `reliability`, `help wanted`
**Depends on:** ADR-005 and DEV-003

Implement the accepted lease protocol and worker loop.

**Acceptance criteria**

- Provide concurrency-safe lease tests.
- Test worker crash, lease expiry, retry, cancellation, and dead-letter paths.
- Expose queue depth, duration, retry, and failure metrics.
- Demonstrate idempotent execution in an integration test.

### DEV-005: Implement the BlobStore interface

**Size:** M
**Suggested label:** `backend`, `storage`, `help wanted`
**Depends on:** ADR-006 and DEV-001

Implement filesystem and S3-compatible adapters behind one application
interface.

**Acceptance criteria**

- Stream data without loading entire reports into memory.
- Verify SHA-256 while writing and reading.
- Enforce configured limits.
- Include conformance tests that run against both adapters.

## First vertical slice

These tasks form the first product milestone and should be implemented in
dependency order.

### MVP-001: Define the versioned ingestion contract

**Size:** M
**Suggested label:** `api`, `architecture`, `ingestion`
**Depends on:** glossary, ADR-001, and ADR-006

Define API operations for creating an import, streaming content, completing an
upload, and retrieving operation status.

**Acceptance criteria**

- Describe the API in OpenAPI.
- Define authentication, idempotency, size limits, and error responses.
- Define asynchronous `202 Accepted` behavior.
- Include examples but no undocumented fields.

### MVP-002: Implement import and raw artifact persistence

**Size:** L
**Suggested label:** `backend`, `ingestion`, `help wanted`
**Depends on:** MVP-001, DEV-003, DEV-004, and DEV-005

Implement the upload path without parsing a report.

**Acceptance criteria**

- Preserve the immutable artifact and metadata.
- Verify idempotent replay.
- Reject oversized, incomplete, or hash-mismatched content.
- Emit audit and telemetry records without logging report content.

### MVP-003: Create licensed SARIF test fixtures

**Size:** S
**Suggested label:** `good first issue`, `testing`, `sarif`
**Depends on:** SEC-002

Create small synthetic fixtures covering representative valid and invalid
SARIF 2.1.0 documents.

**Acceptance criteria**

- Cover multiple runs, rules, locations, fingerprints, and absent optional
  fields.
- Include malformed, oversized, and deeply nested cases.
- Document fixture origin and license.
- Include no real secrets or private source paths.

### MVP-004: Implement the first SARIF adapter

**Size:** L
**Suggested label:** `backend`, `sarif`, `help wanted`
**Depends on:** MVP-002, MVP-003, and the observation schema

Parse supported SARIF 2.1.0 data into immutable observations.

**Acceptance criteria**

- Use bounded, defensive parsing.
- Preserve source rule IDs, levels, messages, locations, and fingerprints.
- Record unsupported fields and partial-import warnings.
- Produce deterministic output for identical input.
- Never execute or render report content as trusted code or markup.

### MVP-005: Implement deterministic correlation

**Size:** L
**Suggested label:** `backend`, `correlation`, `help wanted`
**Depends on:** ADR-002, ADR-003, and MVP-004

Correlate SARIF observations into stable findings.

**Acceptance criteria**

- Store fingerprint algorithm and version.
- Prevent duplicate findings during import replay.
- Preserve collisions for investigation instead of silently discarding data.
- Include tests for moved files, changed line numbers, renamed rules, and
  repeated scans.

### MVP-006: Implement scan reconciliation

**Size:** L
**Suggested label:** `backend`, `domain-model`, `help wanted`
**Depends on:** ADR-001 and MVP-005

Apply present, absent, and reopened transitions using accepted scan-scope
semantics.

**Acceptance criteria**

- Partial and failed scans cannot resolve findings by omission.
- Repeated reconciliation is idempotent.
- Every lifecycle transition produces history and audit records.
- Tests cover full, partial, incremental, failed, and cancelled scans.

### MVP-007: Expose the findings query API

**Size:** M
**Suggested label:** `api`, `backend`
**Depends on:** MVP-005

Provide workspace-scoped finding and observation queries.

**Acceptance criteria**

- Implement cursor pagination and stable ordering.
- Filter by application, lifecycle, disposition, severity, scanner, and time.
- Return source and effective severity separately.
- Enforce authorization before query execution.

### MVP-008: Build the minimal findings UI

**Size:** L
**Suggested label:** `frontend`, `help wanted`
**Depends on:** MVP-007 and the frontend foundation

Display findings and their supporting observations without implementing a full
dashboard suite.

**Acceptance criteria**

- Provide list, filters, details, evidence, and history views.
- Escape all imported content.
- Make loading, partial-data, empty, and error states explicit.
- Meet baseline keyboard and screen-reader accessibility expectations.

## Tasks intentionally not ready

The following should not be implemented until the first vertical slice and its
contracts are stable:

- microservice extraction;
- Kafka or another mandatory message bus;
- a graph database;
- machine-learning correlation;
- a general-purpose plugin marketplace;
- scanner orchestration;
- broad compliance dashboards;
- automatic source-code remediation;
- dozens of vendor connectors.

Discussion, research, and ADR proposals for these topics are welcome, but early
code would create compatibility obligations before the core model is proven.
