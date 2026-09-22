# Ingestion API v1 contract

The normative machine-readable contract is
[`openapi.yaml`](openapi.yaml). It uses OpenAPI 3.1.1 and JSON Schema
2020-12. No server implementation is included yet.

## Workflow

```text
POST import reservation       -> 201 Import(awaiting_upload)
PUT report byte stream        -> 200 UploadReceipt(uploaded)
POST complete                 -> 202 Operation(queued)
GET operation                 -> 200 queued | running | terminal
GET import                    -> current import state
```

The raw report is opaque to the HTTP upload handler. Parsing, scan creation,
normalization, and reconciliation occur asynchronously and remain server-side.
One import represents one immutable raw artifact and may later produce multiple
scans.

## Versioning

The `/api/v1` path is the public compatibility boundary. Additive optional
fields and new enum values require normal compatibility review; clients must
still treat unknown response fields and enum values defensively. Removing or
renaming fields, changing their meaning, or changing workflow semantics
requires a new API path version.

The OpenAPI document's `info.version` versions the contract document. It is not
the server release version.

## Authentication and authorization

Requests use a bearer token only in the `Authorization` header. Required
capabilities are recorded in each operation through `x-required-capabilities`:

| Operation | Capability |
| --- | --- |
| Reserve an import | `imports:create` |
| Upload or complete | `imports:upload` |
| Read own import or operation | `imports:read-own` |

Capability checks intersect principal, token, workspace, and application
scope. IDs and links are not credentials. A resource outside the effective
scope is normally hidden with `404`; a known operation that is forbidden for a
non-concealment reason returns `403`.

## Idempotency

`createImport` and `completeImport` require `Idempotency-Key`. The key scope is:

```text
authenticated principal + workspace + API major version + operation + key
```

The server retains a completed entry for at least 24 hours. A replay with the
same validated request fingerprint returns the original HTTP status, relevant
headers, and response body. The key is not a global import identity. Reuse with
a different fingerprint returns `409 idempotency-key-conflict`; an unresolved
concurrent replay returns `409` with `Retry-After`.

The header syntax and behavior are defined by this contract. They are not
claimed to implement the expired IETF Idempotency-Key draft.

Upload uses `PUT` to the reserved content resource. An interrupted stream never
publishes partial bytes and may be restarted in full before expiration. Once
verified bytes exist, identical replay returns the existing receipt and
different content cannot replace them.

## Streaming and limits

The reservation response returns the effective `max_bytes` and upload expiry.
`Content-Length` is optional. The server rejects a declared oversize request
before consuming it and enforces the same bound while streaming when the length
is missing or false. It computes SHA-256 over the stored bytes and verifies any
client-declared exact size and digest.

Implementations must also enforce server-configured upload duration,
minimum-progress, concurrency, and workspace quota controls. These operational
values are deliberately not hard-coded in the versioned API. Limit failures use
`413`; rate or concurrency limits use `429` and `Retry-After`; incomplete
transport has no successful receipt.

## Asynchronous status

Completing an upload returns `202 Accepted`, a `Location` header, and an
operation resource. `202` confirms queue acceptance only. Clients poll the
operation URI and honor `Retry-After`. Terminal states are `succeeded`,
`failed`, and `cancelled`. A terminal failure exposes a bounded stable code and
safe guidance, never parser stack traces, report excerpts, storage locations,
or credentials.

## Errors and request examples

Errors use `application/problem+json` and the RFC 9457 fields plus stable
`code`, `request_id`, and optional validation `errors`. Custom problem types use
the `urn:open-aspm:problem:<code>` namespace until a stable documentation origin
exists. Clients branch on `status`, `type`, or `code`, not human-readable
`detail`.

Examples in `openapi.yaml` contain only fields described by their schemas.

## Deliberately deferred

- direct-to-object-store and multipart upload;
- resumable byte ranges;
- client-supplied storage keys;
- synchronous parsing or finding creation;
- cancellation and deletion endpoints;
- raw artifact download;
- non-SARIF formats; and
- client declarations that make a scan authoritative for absence.

Scan result, completeness, and normalized scope are still mandatory domain
data under ADR-0001. For this first SARIF slice they are produced conservatively
by the versioned parser; missing evidence becomes `unknown`, never an implied
full successful scan.
