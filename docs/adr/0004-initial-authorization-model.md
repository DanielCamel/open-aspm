# ADR-0004: Initial authorization model

- **Status:** Accepted
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** [#12](https://github.com/spectremi/open-aspm/issues/12)
- **Supersedes:** None
- **Superseded by:** None

## Context

Open ASPM stores sensitive vulnerability evidence, integration credentials,
exceptions, policy decisions, and audit records. A valid identity must not imply
unrestricted access to a workspace or every application inside it.

The initial model must support human users, CI service accounts, outbound
agents, and background workers without embedding provider-specific identities
or a fixed role name in domain logic. It must also permit a small single-team
installation without requiring a separate policy service.

## Decision

### 1. Authentication and authorization are separate

Authentication establishes an actor identity. Authorization evaluates whether
that actor may perform a capability on a resource in a workspace.

- OIDC is the primary interactive authentication protocol.
- Scoped service accounts and API tokens authenticate automation.
- Agents use a separate enrolled workload identity and cannot act as users.
- Background jobs carry an initiating actor and an explicit system capability;
  they do not inherit an unrestricted database identity as authorization.

Successful authentication never grants implicit workspace access.

### 2. Human identity uses issuer and subject

An OIDC user is identified by the pair:

```text
(normalized_issuer, subject)
```

Email address, display name, preferred username, and group name are attributes,
not stable identity. Email changes do not create a new user, and matching email
addresses from different issuers do not merge users.

OIDC login requires authorization-code flow with PKCE, state, and nonce.
Redirect URIs are configured exactly. Tokens and authentication claims are
validated for issuer, audience, signature, time bounds, and the expected flow.

### 3. Workspace membership is explicit

A user or service account can access a workspace only through an active
membership or a narrowly scoped system assignment:

```text
Membership
  workspace_id
  principal_id
  role_bindings[]
  state: invited | active | suspended | removed
  valid_from
  valid_until?
```

Suspension and removal take effect independently of the upstream identity
provider session. Historical ownership and audit attribution retain the
principal ID after membership removal.

### 4. Authorization is capability-based RBAC with resource scope

Roles are named bundles of capabilities. Authorization evaluates:

```text
principal
workspace
capability
resource type
resource ID
application scope
context
```

The initial built-in roles are defaults, not hard-coded conditionals:

| Capability group | Viewer | Developer | Analyst | Application owner | Security manager | Workspace admin |
| --- | --- | --- | --- | --- | --- | --- |
| Read assigned application posture | Yes | Yes | Yes | Yes | Yes | Yes |
| Read raw evidence | No | Scoped | Scoped | Scoped | Yes | Yes |
| Comment and link remediation | No | Scoped | Scoped | Scoped | Yes | Yes |
| Change workflow state | No | Scoped | Scoped | Scoped | Yes | Yes |
| Set triage disposition | No | No | Scoped | Scoped | Yes | Yes |
| Request an exception | No | Scoped | Scoped | Scoped | Yes | Yes |
| Approve or revoke exceptions | No | No | No | No | Yes | Yes |
| Manage application ownership | No | No | No | Scoped | Yes | Yes |
| Manage policies and Gates | No | No | No | No | Yes | Yes |
| Manage integrations | No | No | No | No | Scoped | Yes |
| Read workspace audit | No | No | No | No | Yes | Yes |
| Manage membership and roles | No | No | No | No | No | Yes |

`Scoped` means the capability applies only to explicitly assigned applications
or integrations. Exact capability names are versioned separately from user-
facing role labels.

Deployments may define stricter custom roles later. A custom role can remove or
combine capabilities but cannot create a capability unknown to the server.

### 5. Deny by default and intersect scopes

Access is allowed only when every required condition succeeds:

1. the principal is active;
2. workspace membership is active;
3. the role contains the requested capability;
4. the role binding covers the resource's application or workspace scope;
5. the resource belongs to the same workspace;
6. contextual constraints such as token scope and expiry pass; and
7. no explicit suspension or revocation applies.

Missing resource scope, unknown capability, ambiguous ownership, and
authorization evaluation errors result in denial.

### 6. Authorization occurs before data access where possible

Application services own authorization. HTTP handlers, GraphQL resolvers if
introduced, workers, and connectors call the same authorization interfaces.

Repository methods require an explicit workspace ID. Collection queries apply
authorized application scope inside the query; they do not load all rows and
filter afterward. Object-level operations validate both workspace and resource
scope before reading sensitive fields.

The web UI may hide unavailable actions for usability, but UI behavior is never
an authorization control.

### 7. Service accounts are first-class principals

A service account belongs to one workspace and has independent role bindings,
status, ownership, expiry policy, and audit identity. It is not a synthetic
human user.

CI ingestion tokens should normally receive only capabilities such as:

```text
imports:create
imports:upload
imports:read-own
gates:evaluate
gates:read-own
```

An ingestion token does not receive permission to read findings or integration
credentials unless separately granted.

### 8. API tokens are high-entropy, scoped, and non-retrievable

API tokens contain at least 256 bits of cryptographically secure random secret
material. A short non-secret prefix identifies the verifier record. The server
stores a keyed verifier, such as HMAC-SHA-256 under a separately managed key,
rather than token plaintext.

The token is displayed once. Its record contains:

```text
principal_id
workspace_id
token_prefix
verifier
capability_scope
application_scope?
created_at
expires_at
last_used_at?
revoked_at?
```

Token scope can only narrow the service account's role bindings. Rotation
creates a new token and allows a bounded overlap; revocation is immediate for
new requests. Tokens are never accepted in query parameters.

### 9. Sessions have bounded lifetime and revocation

Interactive sessions use secure, HTTP-only, same-site cookies. State-changing
browser requests require CSRF protection appropriate to the chosen session
design. Session records have idle and absolute expiry and can be revoked when a
membership or identity is suspended.

Authentication tokens, session IDs, authorization headers, and cookies are
redacted from logs, traces, audit details, error responses, and support bundles.

### 10. Integration credentials have separate capabilities

Reading integration configuration does not grant access to credential values.
Credentials remain non-retrievable through normal APIs. Connector execution
requests a credential reference through a narrow server-side interface after
authorization.

The capability to configure a connector, execute it, view its safe health
status, and rotate its credentials are distinct.

### 11. Exceptions require separation from ordinary triage

Changing workflow state or marking a finding confirmed does not grant the
ability to approve accepted risk or policy exceptions. Exception approval and
revocation require explicit capabilities and always record actor, reason,
scope, expiry, and affected policy version.

An installation may permit a workspace administrator to hold every capability,
but the model preserves separation so organizations can assign independent
security managers.

### 12. Audit access is separately authorized

Security-relevant actions record the effective principal, authenticating
principal when different, workspace, capability, target, result, request ID,
and safe reason. Audit records do not copy credentials or unrestricted evidence.

Reading audit history requires `audit:read`. Export and retention administration
use separate capabilities.

### 13. Database constraints are mandatory; RLS is a readiness gate

The initial schema uses workspace columns, compound uniqueness, and foreign-key
validation to prevent cross-workspace relationships. Application-layer
authorization remains mandatory regardless of database controls.

PostgreSQL Row Level Security is not required for the first single-workspace
vertical slice. Open ASPM must not claim hardened multi-tenant isolation until:

- RLS policies or an equivalent independently enforced database boundary cover
  tenant-owned tables;
- connection-pool transaction context cannot leak between workspaces;
- migrations, workers, and administrative operations use separate database
  roles; and
- cross-workspace negative tests pass against the real database.

RLS is defense in depth and never replaces capability authorization.

### 14. Cache, jobs, and idempotency preserve authorization scope

Cache keys, job payloads, operation IDs, idempotency keys, and projections
include workspace scope. Jobs capture the initiating principal and capability
context but revalidate current authorization before delayed external mutation
where revocation must take effect.

An operation status endpoint verifies access to the operation; possession of an
opaque operation ID is not authorization.

## Alternatives considered

### Workspace-wide roles only

Rejected because developers and application owners should not automatically
access unrelated applications or their raw evidence.

### Email address as user identity

Rejected because email is mutable, can be reassigned, and is not globally
unique across issuers.

### Put authorization only in HTTP middleware

Rejected because workers, connectors, and future protocols would bypass it, and
collection filtering requires repository-aware scope.

### Store recoverable API tokens for convenience

Rejected because database or support-access compromise would immediately expose
live credentials. Tokens are rotated, not retrieved.

### Require a separate policy engine initially

Not selected. A typed in-process capability evaluator is simpler for the first
deployment. Its interface may later be backed by an external policy engine
without changing domain capability names.

### Depend exclusively on PostgreSQL RLS

Rejected because RLS does not model all application capabilities or external
side effects and can be bypassed by privileged database roles.

## Consequences

### Positive

- Human, automation, and agent identities remain distinct.
- Authorization is consistent across APIs and workers.
- Application-scoped access reduces unnecessary exposure.
- Token compromise is limited by explicit scope and expiry.
- The model supports separation of triage and exception approval.

### Negative and trade-offs

- Repository queries and background work require explicit authorization context.
- Role-to-capability migrations need compatibility care.
- Application assignments add administrative work.
- Hardened multi-tenancy remains a separate readiness milestone.

## Security and privacy impact

This decision protects vulnerability evidence, credentials, policy decisions,
and audit records from over-broad access. OIDC claims and access history are
personal data subject to retention and export policy. Authentication and
authorization failures return non-enumerating errors where revealing resource
existence would cross a scope boundary.

Workspace administrators can grant powerful capabilities and are high-value
accounts. Deployments should require strong upstream authentication and support
session revocation. Future privileged operations may require step-up
authentication without changing the capability model.

## Compatibility and migration

Capability identifiers are versioned server contracts. Renaming a capability
requires migration of role definitions and tokens. New capabilities default to
denied for custom roles.

Public resource APIs use Open ASPM principal and workspace identifiers. OIDC
issuer and subject remain internal identity attributes and are not accepted as
authorization scope supplied by clients.

## Verification

Tests must include:

- a role-to-capability matrix for every protected operation;
- cross-workspace and cross-application object access;
- collection filtering and pagination under restricted scope;
- suspended membership, expired role binding, revoked token, and expired
  session behavior;
- token scope narrower than service-account role scope;
- credential and audit capabilities independent from ordinary read access;
- job and operation status authorization; and
- log and error redaction.

Authorization tests use real repository queries for negative cases. Handler
tests alone are insufficient.

## Open questions

- Which OIDC group-mapping features belong in the first release?
- Should exception approval support configurable two-person control?
- Which operations require step-up authentication?
- What RLS rollout belongs in the first release claiming multi-tenant support?
