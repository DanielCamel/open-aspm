# ADR-0007: Finding identity and fingerprint versioning

- **Status:** Accepted
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

Correlation identity is different from scan scope. A fingerprint associates
an observation with a finding; scan scope determines whether a later scan may
mark that finding absent.

The previous glossary used "Finding identity" for the versioned deterministic
value. This decision separates two concepts that must not be interchangeable:
the opaque Finding ID and the versioned correlation fingerprint. The glossary
is updated in the accepting change so clients and implementations do not use
both meanings concurrently.

## Decision

### 1. Findings have opaque IDs

Every finding receives an immutable Open ASPM ID scoped to one workspace. The
ID contains no scanner, asset, location, or vulnerability data.

A fingerprint is a correlation key, not the Finding ID. A finding may have
more than one fingerprint over its lifetime. Public contracts expose the
opaque Finding ID; clients do not construct or select correlation
fingerprints.

### 2. Source identity is preserved separately

Observations retain scanner-provided result IDs, rule IDs, and fingerprints
with their provenance. Open ASPM does not treat those values as globally unique.

Open ASPM derives its own fingerprint from normalized inputs:

```text
FindingFingerprint
  workspace_id
  target_type
  target_id
  analysis_kind
  algorithm
  version
  digest
  inputs
```

The target is an opaque Open ASPM catalog identity, never a display name, URL,
path, or provider locator. The algorithm name and version are always stored
with the digest. Fingerprints are compared only within the same workspace,
target type and identity, analysis kind, algorithm, and version.

### 3. Version-one fingerprint families and inputs

Fingerprint families are activated independently. A family may run only when
all of its required normalized identities are available. The initial
deterministic families use these inputs:

```text
SAST
  repository identity
  scanner family
  rule identity
  normalized relative path
  stable source context

SCA
  selected target type and identity
  version-independent package coordinate
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

For SCA, target selection has a fixed precedence. A known component identity
is selected first. Otherwise, a known artifact with a verified immutable
content identity is selected. When both exist, the component is the target and
the artifact remains attributed evidence. When neither exists, correlation is
`uncorrelated` rather than selecting a mutable locator.

The version-one package coordinate is the server-normalized tuple of package
ecosystem/type, namespace, and name. For a Package URL, `version`, `qualifiers`,
and `subpath` are excluded. An observed package version and dependency path are
also evidence, not part of this coordinate. If an ecosystem cannot distinguish
packages safely without a qualifier, version one records the Observation as
`uncorrelated` instead of dropping the qualifier and risking a merge. Direct
versus transitive dependency is evidence rather than version-one finding
identity. A change to these rules requires a new fingerprint version.

### 4. Mutable data is not identity

The following values do not participate in version-one fingerprints:

- severity, confidence, message, and timestamps;
- line and column numbers;
- scanner version and scanner instance;
- repository name or URL;
- mutable image tags;
- workflow, triage, and lifecycle state;
- raw secret value or a digest of that value.

Paths and routes are normalized by versioned server-side code. Adapters retain
and map source facts; they do not select fingerprint inputs, algorithms, or
versions.

When any required input is missing or unsafe, correlation records a durable,
queryable outcome rather than using a weaker fallback:

```text
CorrelationOutcome
  workspace_id
  observation_id
  algorithm
  version
  state: uncorrelated
  reason_codes
```

Version one records every applicable code once in this canonical order:

```text
target_identity_unknown
analysis_kind_unknown
scanner_family_unknown
rule_identity_unknown
package_identity_unknown
vulnerability_identity_unknown
location_identity_unknown
source_context_unknown
source_context_unsafe
```

An algorithm family uses only the codes relevant to its required inputs. If an
ecosystem-specific package qualifier is required to avoid conflation, it emits
`package_identity_unknown`. The exact outcome key is workspace, Observation,
algorithm, and version. Exact replay returns the existing outcome; conflicting
replay is an error. An uncorrelated Observation does not create a stable
Finding because its logical identity is not reproducible. Authorized evidence
and review queries must surface it separately so unknown identity cannot hide
a valid security statement.

### 5. Fingerprints are deterministic and explainable

Each algorithm version defines its required inputs, normalization rules,
canonical encoding, and test vectors. Version one uses SHA-256 over a canonical
encoding of the typed inputs.

The normalized inputs are retained so a match can be explained and reproduced.
The algorithm contract defines an unambiguous typed canonical encoding and
published test vectors; concatenating unescaped source strings is not a valid
encoding. Untrusted report fields cannot choose an algorithm or its version.

Secret correlation has an additional boundary. Stable source context is
eligible only after secret-aware server canonicalization proves that it
contains neither the raw secret nor a reversible or equality-testable
derivative of it. A provider-issued opaque location key or syntax context that
excludes the token may be eligible. Hashing the secret does not make it
eligible. If safe context cannot be constructed, the outcome is
`uncorrelated` with a `source_context_unsafe` reason. Disallowed values must not
enter retained inputs, indexes, logs, metrics, errors, or diagnostics.

### 6. Collisions do not silently merge findings

If a digest matches but its retained canonical inputs do not match byte for
byte, or an active fingerprint mapping points to incompatible target or
finding records, Open ASPM records a correlation conflict. It keeps the
observations and does not automatically attach, discard, or merge them.

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
- Adapters must preserve and deterministically map required source facts, while
  versioned server normalization and correlation own canonicalization and
  fingerprint construction.
- Algorithm migration requires additional storage and review logic.

## Security and privacy impact

Fingerprint inputs may reveal repository paths, packages, routes, and internal
asset relationships. They are protected as workspace data and must not appear
in unauthenticated errors or metric labels.

Manual aliases, merges, and splits require authorization and audit records.
Fingerprint comparison is never an authorization check.

## Compatibility and migration

The glossary definitions distinguish **Finding ID** (the opaque managed
identity) from **correlation fingerprint** (the versioned association key).

Old and new fingerprint versions may coexist during migration. Before a new
version is activated, its effect on unchanged, split, merged, conflicting, and
uncorrelated findings must be reviewed.

## Verification

Tests must cover deterministic replay, workspace isolation, line movement,
scanner changes, dependency upgrades, different image digests, route
normalization, secret rotation, missing inputs, collisions, and migration
between algorithm versions.

SCA tests must cover component-over-artifact target precedence, absence of both
target identities, version-independent package coordinates, and unchanged
identity across direct/transitive dependency evidence. Missing-input tests must
prove that the reason is queryable and replay-idempotent. Secret tests must
prove that neither the secret nor a reversible or equality-testable derivative
appears in retained inputs, indexes, logs, metrics, errors, or diagnostics.

## Open questions

- Which authorized ingestion or catalog contract supplies the repository,
  component, artifact, or deployment target for the first SARIF family? The
  current import reservation supplies only an application identity.
- Which SARIF partial fingerprints are stable enough for the first adapter?
- Which evidence is sufficient to create an automatic alias after a file rename?
