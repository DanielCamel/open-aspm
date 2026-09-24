# Architecture Decision Records

Open ASPM uses Architecture Decision Records (ADRs) for decisions that affect
public contracts, domain invariants, trust boundaries, persistence, deployment,
or long-term maintainability.

## File naming

ADRs use an immutable four-digit sequence and a short descriptive slug:

```text
0001-scan-scope-and-reconciliation.md
0002-asset-identity.md
```

Numbers are never reused, even when a proposal is rejected.

Backlog identifiers such as `ADR-006` in `CONTRIBUTOR_TASKS.md` are planning
task identifiers and do not reserve an ADR file number. The permanent sequence
is assigned when the ADR document is created.

## Status lifecycle

```text
Proposed -> Accepted -> Superseded
             |
             +-------> Deprecated

Proposed -> Rejected
```

- **Proposed:** open for review and not yet an implementation contract.
- **Accepted:** approved and expected to guide implementation.
- **Rejected:** considered but not selected.
- **Deprecated:** retained for compatibility or historical context but should
  not be used for new work.
- **Superseded:** replaced by another ADR.

Merging an ADR marked `Accepted` represents approval of the decision. Merging a
`Proposed` ADR publishes it for continued design discussion but does not make
dependent implementation ready automatically.

## Changing a decision

Accepted ADR text is historical evidence and should not be rewritten to make a
new decision appear old. Material changes require a new ADR that:

1. references the previous ADR;
2. describes the changed context;
3. states compatibility and migration consequences; and
4. marks the previous ADR as superseded.

Small corrections and clarifications that do not change the decision may be
made in place.

## Review expectations

An ADR should include:

- the problem and constraints;
- the decision and invariants;
- realistic alternatives;
- positive and negative consequences;
- security and privacy impact;
- compatibility and migration impact;
- unresolved questions.

Use [template.md](template.md) when proposing a decision.

## Accepted decisions

- [0001: Scan scope and reconciliation](0001-scan-scope-and-reconciliation.md)
- [0002: Asset identity and alias history](0002-asset-identity-and-alias-history.md)
- [0003: PostgreSQL job leasing](0003-postgresql-job-leasing.md)
- [0004: Initial authorization model](0004-initial-authorization-model.md)
- [0005: Raw artifact storage and retention](0005-raw-artifact-storage-and-retention.md)
- [0006: PostgreSQL access and schema migrations](0006-postgresql-access-and-migrations.md)
- [0007: Finding identity and fingerprint versioning](0007-finding-identity-and-fingerprint-versioning.md)
