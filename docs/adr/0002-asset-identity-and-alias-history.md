# ADR-0002: Asset identity and alias history

- **Status:** Accepted
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** [#5](https://github.com/spectremi/open-aspm/issues/5)
- **Supersedes:** None
- **Superseded by:** None

## Context

Open ASPM correlates evidence over time and across integrations. Display names,
URLs, repository paths, container tags, project names, and cloud resource names
are mutable and sometimes reused. Using them as primary identities would split
one asset after a rename or merge unrelated assets after name reuse.

At the same time, identifiers from an external provider are meaningful only in
that provider's namespace and may be unavailable in uploaded reports.

The model must support repository rename and transfer, forks, monorepos,
multiple applications per repository, immutable build artifacts, redeployment,
provider deletion, and later rediscovery.

## Decision

### 1. Internal identity is opaque and workspace-scoped

Every catalog object receives an immutable Open ASPM identifier. The identifier
contains no business meaning and is never derived from a display name or URL.

All tenant-owned identities are scoped by `workspace_id`. Database uniqueness
and foreign-key relationships include or validate workspace scope so an object
from one workspace cannot be attached to another workspace's graph.

The exact opaque identifier encoding is an implementation choice. Public
contracts must not depend on creation time, sort order, or embedded type bits.

### 2. External identities are first-class records

Provider identity is stored separately:

```text
ExternalIdentity
  id
  workspace_id
  provider_type
  provider_instance_id
  object_type
  external_id
  canonical_locator?
  observed_name?
  valid_from
  valid_until?
  last_verified_at
  provenance
```

The principal uniqueness invariant is:

```text
(workspace_id, provider_instance_id, object_type, external_id)
```

`external_id` is the provider's stable opaque identifier when one exists, such
as a repository node ID. URLs and names are locators and aliases, not primary
identity.

Provider instance is included because two self-hosted installations may issue
the same local numeric ID.

### 3. Alias history is temporal

Names, URLs, paths, tags, and other locators are recorded with validity
intervals and provenance. Updating a locator closes the previous interval and
adds a new record; it does not rewrite history.

Alias reuse does not automatically transfer identity. If a repository path is
deleted and later reused for a provider object with a different stable ID, Open
ASPM creates or associates a different repository.

### 4. Application identity is curated, not inferred from repositories

An application is a business object with its own identity and ownership. It is
created explicitly or imported from an authoritative application catalog.

Repository, component, artifact, and deployment relationships do not silently
create application equivalence. One repository may contribute to several
applications, and one application may use several repositories.

### 5. Components model monorepo and shared-code boundaries

A component is related to a repository through an explicit, temporal
relationship that may include a path partition:

```text
ComponentSource
  component_id
  repository_id
  path_prefix?
  valid_from
  valid_until?
```

Path prefixes are normalized locators, not component identities. Moving a
component inside a repository updates the relationship without replacing the
component.

Shared libraries may relate to multiple applications. No implicit ownership is
inferred from directory layout.

### 6. Forks are distinct repositories with lineage

A fork has its own provider external ID and therefore its own repository
identity. Fork ancestry is represented as a relationship:

```text
RepositoryRelation
  source_repository_id
  target_repository_id
  relation_type: forked_from | mirrored_from | supersedes
  provenance
```

Findings are not automatically shared between a fork and its parent. A future
correlation strategy may use lineage as evidence, but must retain distinct
asset identity.

### 7. Artifacts prefer immutable content identity

Artifacts are identified using a type-appropriate immutable digest where the
ecosystem provides one:

```text
ArtifactIdentity
  artifact_type
  digest_algorithm
  digest
  platform?
```

Examples include an OCI manifest digest or a verified package checksum. The
algorithm is part of the identity. A tag, filename, version label, or registry
path is a temporal alias.

Where no trustworthy digest exists, Open ASPM assigns an internal identity and
marks the external identity confidence explicitly. It does not pretend a
mutable locator is content-addressed.

### 8. Deployments connect artifacts to runtime context

A deployment is not an artifact alias. It represents an artifact or component
version operating in an environment during a time interval:

```text
Deployment
  workspace_id
  environment_id
  deployment_slot
  artifact_id?
  component_id
  observed_version?
  first_observed_at
  last_observed_at
  retired_at?
```

Provider external IDs are attached when a deployment platform exposes them.
Updating a mutable deployment slot to a new digest changes the deployment
state while retaining history.

### 9. Conflicts do not silently merge

When evidence associates one provider external ID with two internal objects, or
two provider identities appear to represent one object, Open ASPM records an
identity conflict for review or deterministic connector resolution.

Merge and split operations are explicit, authorized, audited domain actions.
They preserve aliases, relationships, and redirect history. A merge never
deletes evidence.

### 10. Deletion uses tombstones before erasure

Provider deletion or loss of access marks an external identity unverified or
ended. It does not immediately delete the catalog object or its findings.

Workspace retention policy controls eventual erasure. Rediscovery with the same
verified provider identity may reactivate the association; reuse of a name with
a different provider ID does not.

### 11. Confidence and provenance are explicit

Every externally derived identity relationship records its source and
confidence:

```text
authoritative
verified
inferred
user_asserted
conflicted
```

Low-confidence inference may propose a relationship but cannot silently merge
catalog objects.

## Examples

### Repository rename

Provider repository ID `R-17` moves from `team/old-name` to `team/new-name`.

Result: the Open ASPM repository ID remains unchanged. The old locator interval
is closed and the new locator is added.

### Repository transfer

Provider ID `R-17` moves to another organization while the provider guarantees
that the ID remains stable.

Result: identity remains unchanged and ownership/locator history is updated.

### Path deleted and reused

`team/api` with provider ID `R-17` is deleted. A new repository with provider ID
`R-91` later uses `team/api`.

Result: two repository identities exist. The shared historical locator is not
sufficient to merge them.

### Monorepo

Repository `R-20` contains `services/payments` and `services/catalog`, owned by
different teams and used by different applications.

Result: one repository relates to two component identities through temporal
path partitions.

### Mutable image tag

`registry/app:latest` first points to digest D1 and later to D2.

Result: D1 and D2 are different artifacts. The tag has two alias observations,
and deployment history records which digest was active.

## Alternatives considered

### Use canonical URL as identity

Rejected because URLs change, can be reused, and may contain credentials or
deployment-specific hostnames.

### Use names scoped only by workspace

Rejected because names collide across providers and do not survive rename or
transfer.

### Derive applications directly from repositories

Rejected because real application topology is many-to-many and often follows
business ownership rather than source layout.

### Use a graph database as the authoritative identity store

Not selected initially. PostgreSQL can enforce workspace and uniqueness
invariants while representing relationships explicitly. A graph projection may
be introduced for traversal workloads later.

### Automatically merge objects using similarity

Rejected as an authoritative operation because mistakes would contaminate
finding correlation and policy results. Similarity may create reviewable
proposals.

## Consequences

### Positive

- Rename and transfer do not fragment history.
- Name reuse does not combine unrelated assets.
- Monorepos and shared components are representable.
- Artifact and deployment identity remain distinct.
- Conflicts are visible and auditable.

### Negative and trade-offs

- Connectors must preserve provider IDs and instance identity.
- Identity history requires temporal records and conflict handling.
- Reports without provider identity may remain ambiguously associated.
- Explicit catalog curation is required for some application relationships.

## Security and privacy impact

Workspace-scoped constraints reduce cross-tenant attachment risk. URLs and
provider metadata can contain internal names and are protected as workspace
data. Credential-bearing URLs must be rejected or redacted before persistence.

Explicit merge permissions are security-sensitive because merging can change
the findings and policies visible on an application. Merge, split, and manual
identity assertions require audit records.

## Compatibility and migration

External identity types and provider namespaces are versioned. Public APIs use
Open ASPM IDs and expose external identities as attributed data.

Historical records created without stable provider identity remain valid but
carry lower confidence. Migrations must not invent provider IDs from mutable
URLs.

## Verification

Tests must cover rename, transfer, deletion and reuse, fork ancestry, monorepo
path moves, shared components, tag reuse, digest changes, conflicting identity
claims, cross-workspace attachment attempts, and replayed connector events.

Property tests should verify that mutable alias changes do not alter internal
identity and that one provider identity cannot be active on two objects in the
same workspace without an explicit conflict.

## Open questions

- Which provider-specific IDs remain stable across installation migration?
- How should canonical package URLs interact with build-specific artifact
  digests?
- What approval model is required for manual merge and split operations?
- When should a retired deployment become eligible for erasure?
