# ADR-0007: Finding identity and fingerprint versioning

- **Status:** Proposed
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** ADR-003 in [CONTRIBUTOR_TASKS.md](../../CONTRIBUTOR_TASKS.md)
- **Supersedes:** None
- **Superseded by:** None

## Context

Open ASPM receives repeated observations from security scanners. It must decide
which observations describe the same finding without merging unrelated issues.

Scanner-provided IDs and fingerprints are useful evidence, but they are not a
stable Open ASPM identity. Their meaning and lifetime differ between scanners
and may change after a scanner or parser upgrade. Mutable values such as line
numbers, severity, and message text are also unsuitable as identity.

Finding identity is different from scan scope. A fingerprint associates an
observation with a finding; scan scope determines whether a later scan may
mark that finding absent.

## Decision

### 1. Findings have opaque IDs

Every finding receives an immutable Open ASPM ID scoped to one workspace. The
ID contains no scanner, asset, location, or vulnerability data.

A fingerprint is a correlation key, not the finding ID. A finding may have
more than one fingerprint over its lifetime.

### 2. Source identity is preserved separately

Observations retain scanner-provided result IDs, rule IDs, and fingerprints
with their provenance. Open ASPM does not treat those values as globally unique.

Open ASPM derives its own fingerprint from normalized inputs:

```text
FindingFingerprint
  workspace_id
  analysis_kind
  algorithm
  version
  digest
  inputs
```

The algorithm name and version are always stored with the digest. Fingerprints
are compared only within the same workspace, analysis kind, algorithm, and
version.

### 3. Version-one fingerprint inputs

The initial deterministic fingerprints use these inputs:

```text
SAST
  repository identity
  scanner family
  rule identity
  normalized relative path
  stable source context

SCA
  component identity, or immutable artifact identity
  package identity
  vulnerability identity

Container
  immutable image digest
  package identity
  vulnerability identity

DAST
  deployment identity
  scanner family
  rule identity
  normalized route
  parameter name and location, when applicable

Secret
  repository identity
  scanner family
  secret type
  normalized relative path
  stable source context
```

Scanner family is included where rule semantics are tool-specific. Version one
does not automatically correlate SAST, DAST, or secret observations across
different scanner families.

### 4. Mutable data is not identity

The following values do not participate in version-one fingerprints:

- severity, confidence, message, and timestamps;
- line and column numbers;
- scanner version and scanner instance;
- repository name or URL;
- mutable image tags;
- workflow, triage, and lifecycle state;
- raw secret value or a digest of that value.

Paths and routes are normalized by versioned server-side code. When required
identity input is missing, the observation remains stored but is not attached
through an unreliable fallback.

### 5. Fingerprints are deterministic and explainable

Each algorithm version defines its required inputs, normalization rules,
canonical encoding, and test vectors. Version one uses SHA-256 over a canonical
encoding of the typed inputs.

The normalized inputs are retained so a match can be explained and reproduced.
Untrusted report fields cannot choose an algorithm or its version.

### 6. Collisions do not silently merge findings

If the same fingerprint points to incompatible evidence, Open ASPM records a
correlation conflict. It keeps the observations and does not automatically
attach, discard, or merge them.

Concurrent creation of the same valid fingerprint must converge on one finding.
Replaying an import or correlation job must not create duplicate findings or
attachments.

### 7. Algorithm changes create new versions

An algorithm version is immutable after use. Changing its inputs,
normalization, or encoding requires a new version.

Migration between versions is explicit:

- a proven one-to-one match may add the new fingerprint to the existing finding;
- one-to-many or many-to-one matches require review;
- existing finding IDs, observations, and history are not rewritten;
- merges and splits are separate authorized and audited actions.

A parser upgrade cannot silently change finding membership. New parser output
is correlated using the declared fingerprint algorithm and version.

## Examples

### SAST line movement

The same rule and source context move from line 42 to line 68 in the same file.

Result: both observations support one finding because line number is not part
of the fingerprint.

### Dependency upgrade

A component upgrades a package, but the same vulnerability still applies.

Result: the new observation supports the same component-level SCA finding. The
observed package version remains evidence.

### Rebuilt container

Two image digests contain the same vulnerable package.

Result: they produce separate container findings because the immutable image
digest is part of the fingerprint.

### Secret rotation

A secret changes at the same stable source location.

Result: the new observation supports the same finding. The secret value is not
stored in or derived into the fingerprint.

## Alternatives considered

### Use scanner fingerprints as finding IDs

Rejected because their scope and stability differ between scanners and versions.

### Hash every observation field

Rejected because ordinary changes to messages, severity, or line numbers would
create unnecessary new findings.

### Automatically correlate across scanners

Rejected for version one because equivalent locations and messages do not prove
that two scanners describe the same issue.

## Consequences

### Positive

- Finding IDs remain stable when algorithms change.
- Correlation is deterministic, versioned, and explainable.
- Common changes such as line movement do not fragment findings.
- Ambiguous matches do not silently contaminate finding history.
- Secret values do not enter correlation indexes.

### Negative and trade-offs

- Some real duplicates remain separate until an explicit alias or merge exists.
- Adapters must implement and test normalization rules.
- Algorithm migration requires additional storage and review logic.

## Security and privacy impact

Fingerprint inputs may reveal repository paths, packages, routes, and internal
asset relationships. They are protected as workspace data and must not appear
in unauthenticated errors or metric labels.

Manual aliases, merges, and splits require authorization and audit records.
Fingerprint comparison is never an authorization check.

## Compatibility and migration

Public APIs use the opaque finding ID. Clients do not construct fingerprints.

Old and new fingerprint versions may coexist during migration. Before a new
version is activated, its effect on unchanged, split, merged, conflicting, and
uncorrelated findings must be reviewed.

## Verification

Tests must cover deterministic replay, workspace isolation, line movement,
scanner changes, dependency upgrades, different image digests, route
normalization, secret rotation, missing inputs, collisions, and migration
between algorithm versions.

## Open questions

- Which SARIF partial fingerprints are stable enough for the first adapter?
- Should SCA identity distinguish direct and transitive dependencies?
- Which evidence is sufficient to create an automatic alias after a file rename?
