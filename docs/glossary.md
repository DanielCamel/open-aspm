# Open ASPM Glossary

## Purpose

This glossary defines the initial domain language used by Open ASPM. The terms
are intentionally independent of database table names and API resource names.
Those implementation contracts may be introduced only after the corresponding
domain invariants are understood.

## Organization and access

### Workspace

The primary isolation boundary for data, identities, configuration, and access
control. Every tenant-owned domain record belongs to exactly one workspace.

A deployment may initially expose only one workspace, but the data model must
not assume that all records belong to a global implicit tenant.

### User

A human identity authenticated through an identity provider or another
supported interactive authentication mechanism.

### Service account

A non-human identity used by CI systems, integrations, and automation. A
service account receives explicit scopes and must not inherit the unrestricted
permissions of the user who created it.

### Role

A named collection of capabilities granted to a user or service account within
a defined scope, such as a workspace or application.

## Application catalog

### Application

A business-recognized software product or service whose security posture is
managed as a unit. An application can contain multiple components,
repositories, artifacts, and deployments.

An application is not assumed to map one-to-one to a repository or a deployed
service.

### Component

A logical, owned, and usually independently versioned part of an application.
Examples include a backend service, web client, library, mobile application, or
infrastructure module.

### Repository

A source-control repository known to Open ASPM through a provider identity. Its
display name, URL, organization, and default branch may change without changing
the identity of the repository.

### Artifact

An output of a build or packaging process, preferably identified by an
immutable digest. Examples include a container image, package, binary, archive,
or infrastructure bundle.

A mutable tag or filename is an alias, not a stable artifact identity.

### Environment

A logical operational context such as development, staging, or production. An
environment may contain many deployments and may have policy attributes such as
criticality or data classification.

### Deployment

The placement of a particular artifact or component version into an
environment. A deployment relates build-time evidence to runtime exposure.

### Owner

A user, team, or external organizational identity responsible for an
application, component, finding, or remediation action.

### External identity

A provider-issued identifier and its history used to associate an Open ASPM
object with an object in another system. External identities allow objects to
survive provider-side rename or transfer operations.

## Integrations and execution

### Integration

A workspace configuration that connects Open ASPM to an external system. It
contains non-secret settings, capability configuration, scheduling information,
and references to credentials.

### Connector

Versioned code that implements a supported external-system protocol and maps
between that protocol and Open ASPM contracts. A connector definition is not an
integration instance and does not contain workspace credentials.

### Agent

An independently deployed Open ASPM process that obtains scoped work through
an outbound connection and accesses systems that the server cannot reach
directly. It does not make authorization, correlation, or policy decisions.

### Operation

A user-visible asynchronous activity with status, progress, errors, and a
stable identifier. One operation may execute several internal jobs.

### Job

A leased unit of background work processed at least once. Job handlers must be
idempotent because a lease can expire and the same job can be delivered again.

## Ingestion

### Raw artifact

The immutable bytes received from a scanner, client, or integration together
with a cryptographic digest and storage metadata. A raw artifact is untrusted
content and is never rendered or executed directly.

### Import

The durable receipt and processing context for one submitted raw artifact. It
records idempotency, provenance, validation results, processing state, and the
resulting scans.

An import is not necessarily a scanner execution: one report may describe
multiple runs, and one scanner execution may produce multiple reports.

### Scan

A scanner's assessment of a declared target and scope at a particular time. A
scan records the tool, configuration identity, completion state, and coverage
needed to interpret both present and missing observations.

### Scan scope

The normalized description of what a scan was authoritative for. It may include
an application, component, repository, branch, commit, artifact digest,
environment, scanner, configuration, and coverage dimensions.

### Full scan

A successful scan declared authoritative for all supported observations within
its compatible scope. Only a compatible full scan may normally make a previous
observation absent by omission.

### Partial scan

A scan that intentionally covers only part of its target. Omission outside its
declared coverage has no lifecycle meaning.

### Incremental scan

A scan that reports changes relative to a baseline rather than a complete
snapshot. Missing observations do not imply resolution unless the format and
connector provide an explicit, authoritative removal event.

## Findings and evidence

### Observation

An immutable statement made by a specific scanner in a specific scan. It
preserves source identifiers, source severity, message, location, evidence, and
provenance.

### Finding

The stable, correlated security issue managed by Open ASPM. A finding can be
supported by observations from multiple scans or tools and has independent
technical, triage, and workflow state.

### Observation and finding example

A SAST tool reports rule `SQL-001` at line 42 in three consecutive scans. Open
ASPM stores three observations because each is evidence from a distinct scan.
If their versioned identity inputs match, all three observations support one
finding. A later compatible full scan with no matching observation may move the
finding to `absent`; it does not delete the earlier observations.

### Finding identity

The versioned, deterministic identity used to associate observations with a
finding. It contains the fingerprint algorithm name and version, not only an
opaque digest.

### Evidence

Source-provided or derived material that supports an observation, such as a
code flow, package identifier, HTTP exchange, scanner explanation, or safe
excerpt. Evidence remains attributable to its source.

### Location

A structured position associated with an observation. Depending on the domain,
it may identify a source region, package, container layer, URL route, cloud
resource, or configuration key.

### Technical lifecycle

The scanner-supported presence of a finding: `detected`, `present`, `absent`,
or `reopened`. It is separate from human triage and remediation workflow.

### Triage disposition

A human or policy-backed interpretation of a finding, initially `unknown`,
`confirmed`, `false_positive`, `accepted_risk`, or `duplicate`.

### Workflow state

The remediation progress of a finding, initially `new`, `assigned`,
`in_progress`, `ready_for_verification`, or `closed`.

### Exception

A time-bounded, attributable decision that changes how a finding or policy
violation is treated. Examples include accepted risk, approved suppression, or
a temporary policy waiver. An exception does not rewrite source evidence.

## Intelligence, risk, and policy

### Vulnerability record

A versioned representation of vulnerability intelligence and its aliases,
affected products, fixes, and source provenance. It is distinct from a finding,
which represents exposure in a workspace asset.

### Enrichment

Information added to source data from another provider or calculation. An
enrichment never silently replaces source facts and retains its own provenance
and observation time.

### Risk assessment

A versioned calculation of risk for a defined subject. It records the algorithm
version, input snapshot, score, factors, and calculation time.

### Policy

A named set of security requirements. Changes create explicit policy versions
so historical evaluations remain reproducible.

### Policy evaluation

The immutable result of applying one policy version to a subject and input
snapshot.

### Security Gate

A policy evaluation intended to control or advise a delivery decision. Its
decision is `passed`, `failed`, `warning`, or `unknown` and includes
machine-readable reasons.

## Platform records

### Domain event

A versioned statement that a meaningful domain change occurred. Domain events
are created in the same transaction as the authoritative state change.

### Transactional outbox

A durable delivery mechanism that stores publishable events in the same
database transaction as domain changes and delivers them asynchronously.

### Audit event

An append-only security record describing who or what performed an action, its
scope, time, result, and relevant identifiers. Audit events must not contain
retrievable credentials or unrestricted report content.

### Projection

A rebuildable read model optimized for queries, dashboards, or export. A
projection is not the authoritative copy of security state.
