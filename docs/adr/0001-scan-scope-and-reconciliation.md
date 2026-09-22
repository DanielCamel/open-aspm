# ADR-0001: Scan scope and reconciliation semantics

- **Status:** Accepted
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** [#4](https://github.com/spectremi/open-aspm/issues/4)
- **Supersedes:** None
- **Superseded by:** None

## Context

Open ASPM receives repeated observations from scanners. It must determine when
a finding remains present, becomes absent, or reopens. Treating every report as
a full snapshot is unsafe: scanners commonly produce partial, incremental,
cancelled, failed, branch-specific, configuration-specific, or filtered
results.

The absence of an observation has meaning only when the new scan was
authoritative for the same security question as the previous observation.

This decision defines domain semantics. It does not require every scanner to
provide every scope dimension. Missing information reduces what Open ASPM may
safely infer.

## Decision

### 1. Scan and import remain separate

An `Import` records receipt and processing of one raw artifact. A `Scan`
represents one scanner assessment described by that artifact.

- One import may contain multiple scans, such as multiple SARIF runs.
- One external scanner execution may produce multiple imports.
- Import success does not imply scan success.
- Parser success does not make a scan authoritative for absence.

### 2. Every scan has explicit result and completeness

```text
ScanResult
  succeeded
  failed
  cancelled
  timed_out
  unknown

ScanCompleteness
  full
  partial
  incremental
  unknown
```

These dimensions are independent. A successfully parsed report may describe a
failed or partial scan.

Only `result = succeeded` and `completeness = full` can infer absence from an
omitted observation. An incremental format may carry an explicit authoritative
removal event, but omission alone has no removal meaning.

### 3. Scan scope is normalized and versioned

Each scan records a versioned `ScanScope`:

```text
ScanScope
  schema_version
  workspace_id
  application_id?
  component_id?
  repository_id?
  branch_ref?
  commit_sha?
  artifact_id?
  artifact_digest?
  deployment_id?
  environment_id?
  scanner_family
  scanner_instance_id?
  scanner_version?
  scanner_configuration_id?
  scanner_configuration_hash?
  analysis_kind
  rule_partition?
  path_partition?
  target_partition?
  coverage_metadata
```

Question marks identify dimensions that may not apply to every analysis kind,
not dimensions that may be silently discarded.

`analysis_kind` distinguishes security questions such as SAST, SCA, secret,
container, DAST, IaC, and cloud configuration analysis. A full SAST scan is not
authoritative for SCA observations.

Partition fields describe intentional subsets. Examples include a monorepo
subdirectory, a subset of rules, one container platform, or selected DAST
routes. A scan cannot resolve findings outside its declared partition.

### 4. Scope identity and evidence identity are distinct

Scope determines where omission may carry meaning. Finding identity determines
which observation corresponds to which finding.

The same finding identity may be observed in several compatible scopes, and a
single scope may contain many finding identities. Scope fields must not be
blindly concatenated into every finding fingerprint.

### 5. Compatibility is explicit and conservative

A new scan `N` may reconcile an observation from prior scan `P` only if all of
the following hold:

1. both belong to the same workspace;
2. `N` succeeded and is full;
3. `N` is ordered after `P` in the same scan lineage;
4. their analysis kinds are equal;
5. their authoritative target identities are equal or `N` explicitly covers
   the narrower target of `P`;
6. branch, commit, artifact, deployment, and environment semantics are
   compatible for that analysis kind;
7. `N` covers every rule, path, and target partition that produced the prior
   observation;
8. scanner-family compatibility rules permit reconciliation;
9. a relevant configuration change has not invalidated comparison; and
10. neither scan has unresolved ambiguity in required scope dimensions.

Compatibility is implemented by a versioned strategy selected for an analysis
kind. It returns a structured result:

```text
compatible
incompatible(reason)
unknown(reason)
```

`unknown` behaves like incompatible for absence inference and remains visible
for diagnostics.

### 6. Scanner compatibility is not assumed globally

By default, one scanner family does not resolve observations produced by a
different scanner family. A future correlation policy may declare specific
families equivalent for a defined rule taxonomy, but that decision must be
versioned and explainable.

Scanner version changes do not automatically create a new scope. Connector
metadata may declare that a version or ruleset change is comparison-breaking.
When compatibility is unknown, Open ASPM preserves the finding as present and
records why reconciliation was skipped.

### 7. Reconciliation uses an explicit lineage order

Source timestamps alone are not trusted to establish state order. Each
compatible scan lineage receives a monotonic server-side reconciliation
sequence when an authoritative scan becomes ready for reconciliation.

If a connector provides a verifiable predecessor, baseline, or monotonically
ordered external run identifier, it is retained and validated. A late import
known to represent an older scan may add historical observations but must not
reverse lifecycle state established by a newer reconciled scan.

When ordering cannot be established safely, observations are imported but
absence inference is skipped.

### 8. Lifecycle changes are event-backed

The initial technical lifecycle is:

```text
detected -> present -> absent -> reopened
```

The normalized events are:

```text
FindingDetected
FindingObserved
FindingBecameAbsent
FindingReopened
```

Events record the responsible scan, observation where applicable, scope
strategy version, reconciliation sequence, and reason.

- The first correlated observation creates `FindingDetected`.
- A later correlated observation creates `FindingObserved` and keeps the
  finding present.
- Compatible authoritative omission creates `FindingBecameAbsent`.
- A new observation after absence creates `FindingReopened`.

Replaying the same import or reconciliation operation must not create duplicate
events.

### 9. Human workflow does not alter technical presence

False-positive disposition, accepted risk, assignment, ticket closure, and
workflow closure do not make a finding technically absent. Conversely,
technical absence does not delete a risk acceptance or its audit history.

### 10. Failed and incomplete work remains observable

Failed, cancelled, timed-out, partial, incremental, and unknown scans are
stored. Their observations may add or reopen findings if the evidence is valid,
but omission never resolves findings unless an explicit authoritative removal
event is supported.

## Examples

### Compatible full SAST scan

Scan A is a successful full SAST scan of repository R, branch `main`, commit X,
and ruleset Q. It observes finding F. Scan B is a later successful full scan of
the same lineage and compatible ruleset at commit Y. B does not observe F.

Result: F may become absent, subject to the SAST scope strategy.

### Failed scan with an empty report

Scan B targets the same repository but reports `failed` and contains no
observations.

Result: F remains present. The failed scan and diagnostic are retained.

### Pull-request incremental scan

Scan B analyzes only changed files in a pull request.

Result: new observations can create or reopen findings. Missing observations
cannot make existing findings absent.

### Monorepo subdirectory scan

Scan A covers `services/payments/**`. Scan B fully covers
`services/catalog/**`.

Result: B is not authoritative for observations from A.

### Container tag reused for a new image

Scan A covers tag `latest` with digest D1. Scan B covers tag `latest` with
digest D2.

Result: artifact identity is based on the digest. Whether D2 supersedes D1 in a
deployment is a catalog and deployment decision, not scan-scope equality.

### Older report imported late

Scan C has already reconciled after Scan B. The report for older Scan A is then
imported.

Result: A's observations are retained as historical evidence. A does not make a
finding present or absent relative to C unless an explicit lineage repair
operation is performed.

## Alternatives considered

### Treat every successful import as a full snapshot

Rejected because import success says nothing about scanner coverage and would
produce false remediation events.

### Never infer absence

Safe but operationally inadequate. Findings would never be verified as fixed
without manual action, eliminating an important ASPM capability.

### Let each connector directly close findings

Rejected because it distributes lifecycle policy across adapters, makes
behavior inconsistent, and prevents central audit and replay.

### Use scanner timestamps as the only ordering source

Rejected because clocks, imports, and provider APIs can be delayed,
misconfigured, or manipulated.

## Consequences

### Positive

- Partial and failed scans cannot silently close findings.
- Lifecycle decisions are explainable and replayable.
- Connector-specific knowledge remains versioned behind scope strategies.
- Late and out-of-order reports retain evidence without corrupting current
  state.

### Negative and trade-offs

- Connectors must provide richer scan metadata.
- Some findings remain present when a scanner cannot prove authoritative scope.
- Reconciliation strategies require domain-specific tests.
- Monorepo and incremental analysis behavior is more complex than snapshot
  replacement.

## Security and privacy impact

Incorrect absence is an integrity failure that can allow vulnerable software to
pass policy. Conservative `unknown` handling reduces this risk at the cost of
manual investigation. Scope metadata may contain repository paths and target
names and is therefore subject to workspace authorization and retention.

## Compatibility and migration

`ScanScope` and reconciliation strategies are versioned. New required scope
dimensions cannot be assumed for historical scans. A migration may enrich old
records where provenance is sufficient, but must not invent authoritative
coverage.

## Verification

Tests must cover full, partial, incremental, failed, cancelled, timed-out, and
unknown scans; rule and path partitions; scanner configuration changes;
branches and commits; artifact digests; monorepos; late imports; retries; and
reopening after absence.

Property tests should verify that non-authoritative scans never produce an
absence event and that replay is idempotent.

## Open questions

- Which analysis kinds require commit ancestry information for compatibility?
- Which formats can express authoritative incremental removal events?
- How should operators intentionally repair an incorrect historical lineage?
- Which scanner upgrades should be declared comparison-breaking by default?
