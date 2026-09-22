# ADR-0005: Raw artifact storage and retention

- **Status:** Accepted
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** [#13](https://github.com/spectremi/open-aspm/issues/13)
- **Supersedes:** None
- **Superseded by:** None

## Context

Open ASPM must preserve the exact scanner report received for provenance,
reprocessing, parser upgrades, and investigation. Reports can be large,
sensitive, malformed, or intentionally malicious. Storing them in PostgreSQL
would couple database growth and backup cost to opaque blobs, while treating an
object store as the domain database would weaken referential integrity and
authorization.

The initial design needs a local development backend and an S3-compatible
production backend with identical integrity, streaming, lifecycle, and security
semantics.

## Decision

### 1. PostgreSQL stores metadata; BlobStore stores bytes

PostgreSQL is authoritative for raw-artifact identity, workspace ownership,
state, provenance, digest, size, retention, and references. A `BlobStore`
contains opaque bytes addressed only through server-generated storage keys.

The application never discovers domain objects by listing a bucket or
directory. Storage listings are used only for repair and orphan cleanup.

### 2. Raw artifacts are immutable after commit

The lifecycle is:

```text
pending -> uploading -> committed -> deletion_pending -> deleted
                    \-> rejected
                    \-> abandoned
```

- `pending` reserves an artifact identity and upload limits.
- `uploading` has an active bounded upload attempt.
- `committed` has verified size and SHA-256 and may be processed.
- `rejected` records safe validation failure metadata.
- `abandoned` represents an incomplete upload past its lease.
- `deletion_pending` is no longer available for new processing.
- `deleted` confirms that retained bytes were removed.

Committed bytes are never modified in place. Replacing content creates a new
artifact and import relationship.

### 3. The server generates every storage key

Clients provide display filenames and format hints only. Storage keys are
opaque, server-generated values and are never constructed from a filename,
repository path, URL, workspace name, or external identifier.

A backend may use a layout similar to:

```text
artifacts/<opaque workspace partition>/<opaque artifact ID>/<opaque object ID>
```

The layout is not a public contract. Workspace prefixes aid operations but are
not an authorization boundary.

Display filenames are normalized metadata, length-limited, and excluded from
HTTP header construction unless safely encoded.

### 4. Initial uploads are server-proxied and streaming

The first ingestion contract streams report content through the Open ASPM API
to the BlobStore. The server:

1. authorizes the import and artifact reservation;
2. enforces declared and observed byte limits;
3. applies an upload deadline and minimum-progress policy;
4. computes SHA-256 while streaming;
5. counts exact stored bytes;
6. asks the backend to atomically commit or promote the staged object; and
7. commits metadata only after storage success.

The server does not buffer the entire report in memory or in a PostgreSQL
column.

Direct browser-to-object-store multipart upload is deferred. It may be added
only with short-lived scoped upload authorization, post-upload checksum and
size verification, abort handling, and a server-side commit step.

### 5. The BlobStore contract is intentionally small

The initial application interface provides operations equivalent to:

```text
Put(ctx, generatedKey, stream, limits) -> size, digest, backendVersion
Open(ctx, generatedKey) -> stream, metadata
Stat(ctx, generatedKey) -> metadata
Delete(ctx, generatedKey, expectedVersion) -> result
```

Exact Go types are decided during implementation. The invariants are:

- application code supplies only generated keys;
- I/O is streaming;
- contexts control cancellation and deadlines;
- writes either become committed objects or remain identifiable staging data;
- delete is idempotent;
- backend error classes are normalized without leaking credentials or internal
  paths; and
- all adapters pass the same conformance suite.

Domain code does not depend on S3 SDK types, filesystem paths, ETags, or vendor
error objects.

### 6. SHA-256 and byte size are mandatory

Every committed raw artifact records:

```text
sha256
size_bytes
media_type_hint
original_filename?
storage_backend
storage_key
backend_version?
created_at
committed_at
```

SHA-256 is calculated from the exact stored bytes. A client-supplied digest may
be used as an expectation but is never accepted without server verification.

An S3 ETag is not treated as a content hash. Backend version IDs are retained
for guarded deletion and recovery when supported.

Workers verify metadata before processing. Full read-time hash verification is
required when backend integrity is uncertain or a repair operation detects a
mismatch; routine verification policy may be optimized later using trusted
backend checksums.

### 7. Limits apply before and during upload

The server defines per-workspace and per-format bounds for:

- declared content length;
- actual streamed bytes;
- upload duration and idle time;
- concurrent uploads;
- stored raw bytes; and
- later decompressed and parsed content.

Missing `Content-Length` does not disable the streaming limit. Exceeding a limit
stops the upload, records a safe rejection code, and schedules staged data for
cleanup.

Compressed and uncompressed limits are independent. An accepted compressed
artifact can still be rejected during bounded extraction.

### 8. Filesystem storage is for development and controlled single-node use

The filesystem adapter is rooted in an operator-configured directory. It:

- creates directories and temporary files with restrictive permissions;
- rejects absolute paths and path traversal;
- does not follow client-controlled symlinks;
- writes to a staging file in the same storage boundary;
- flushes and atomically renames on commit where the platform supports it;
- never returns host filesystem paths through the API; and
- refuses to use the repository checkout or current working directory as the
  default artifact root.

The directory must not be served directly by a web server.

### 9. S3-compatible storage is private and encrypted

Production object storage requirements are:

- private buckets with public access disabled;
- TLS for network transport;
- server-side encryption, with operator-managed KMS keys where required;
- least-privilege credentials restricted to the configured bucket and prefix;
- bucket versioning or guarded deletes where supported;
- no credentials embedded in object URLs, logs, or database records; and
- lifecycle rules coordinated with, but not substituted for, application
  retention state.

Open ASPM initially proxies authorized downloads. Future presigned downloads
must be short-lived, single-object, content-disposition safe, and issued only
after application authorization and audit.

### 10. Authorization is evaluated for every domain access

BlobStore keys are never accepted directly from public API clients. A request
identifies an import or artifact domain ID. The application verifies workspace,
resource scope, and a capability such as `raw_evidence:read` before resolving a
storage key.

Possession of a storage key, object URL, checksum, or import ID is not
authorization.

Workers receive artifact IDs in jobs and resolve storage through a scoped
application service. Job payloads do not contain object-store credentials.

### 11. Retention is metadata-driven and asynchronous

Retention policies independently cover raw artifacts and normalized domain
records. A policy produces a deletion eligibility time; it does not immediately
delete bytes in the transaction that changes domain state.

Deletion proceeds as follows:

1. verify retention expiry and absence of legal hold;
2. mark the artifact `deletion_pending` in PostgreSQL;
3. enqueue an idempotent deletion job;
4. delete the expected backend object or version;
5. record the result and transition to `deleted`; and
6. retain minimal non-sensitive tombstone metadata required for audit and
   referential integrity.

A failed delete remains retryable and visible. Database metadata is not erased
first.

### 12. Legal hold overrides ordinary retention

A legal or investigation hold records actor, reason, scope, creation time, and
release authorization. It prevents deletion but does not widen read access.

Release of a hold re-evaluates normal retention; it does not directly delete
content inside the authorization request.

### 13. Backup and restore treat metadata and bytes as one recovery set

Operators need a recovery point that covers PostgreSQL metadata and referenced
objects. Documentation must define acceptable consistency windows and restore
order.

After restore, a reconciliation operation can identify:

- metadata referencing a missing object;
- objects without committed metadata;
- size, version, or checksum mismatches; and
- deletion-pending artifacts requiring continuation.

Orphan cleanup uses a grace period longer than the maximum upload, retry, and
backup window. It never deletes a recently staged object merely because a
single database read did not find metadata.

### 14. Raw content is not rendered or indexed by default

Raw artifacts are input to bounded parsers. They are not served in the
application origin, indexed into general search, included in support bundles,
or copied into logs and traces.

Authorized download uses an attachment content type and safe filename. Browser
preview of scanner content is deferred until a format-specific, sandboxed
rendering design exists.

## Alternatives considered

### Store report bytes directly in PostgreSQL

Rejected as the default because large opaque objects would amplify database
backup, replication, vacuum, and query operational cost. Small normalized
evidence remains appropriate for relational storage.

### Let clients choose object keys

Rejected because it creates path traversal, overwrite, enumeration, and
cross-workspace collision risks.

### Upload directly to S3 in the first version

Deferred because secure multipart commit, checksum verification, abandoned
upload cleanup, and provider compatibility would expand the first ingestion
contract. Server-proxied streaming is simpler and still bounded.

### Treat S3 ETag as SHA-256

Rejected because ETag meaning varies with multipart upload, encryption, and
provider implementation.

### Delete metadata and rely on bucket lifecycle

Rejected because deletion would become difficult to audit and restore, legal
hold could be bypassed, and dangling domain references would be hidden.

## Consequences

### Positive

- Exact source evidence survives parser and normalization upgrades.
- PostgreSQL remains focused on relational state and integrity.
- Local and S3-compatible deployments share one application contract.
- Size, digest, retention, and deletion decisions are auditable.
- Storage paths and credentials remain outside public APIs.

### Negative and trade-offs

- Backup and restore require coordination across two storage systems.
- Staging and orphan cleanup add operational state.
- Server-proxied uploads consume API bandwidth in the initial release.
- Deletion is eventually consistent rather than immediate.

## Security and privacy impact

Raw reports may contain source code, internal URLs, credentials, personal data,
and unpatched vulnerability details. They receive stricter access than ordinary
finding summaries and are subject to workspace retention and audit.

Encryption does not replace authorization. Hashes can reveal equality and are
not public identifiers. Error handling must avoid returning storage keys,
filesystem paths, bucket names, request signatures, or provider responses that
contain credentials.

## Compatibility and migration

The domain stores a backend-neutral storage reference version. Moving content
between backends uses a verified copy operation that preserves artifact ID,
size, SHA-256, and audit history while atomically changing the active storage
reference.

BlobStore adapter behavior is governed by conformance tests. Adding multipart
or direct upload requires a new accepted ADR if it changes trust boundaries or
the public ingestion protocol.

## Verification

The conformance suite must test:

- streaming content larger than internal buffer sizes;
- exact size and digest calculation;
- cancellation and deadline behavior;
- oversized and incomplete upload cleanup;
- generated-key enforcement and path traversal attempts;
- atomic visibility of committed content;
- idempotent guarded delete;
- cross-workspace access denial;
- filesystem symlink and permission handling;
- S3 multipart and ETag non-assumptions where applicable;
- retention, legal hold, retry, and restore reconciliation; and
- redaction of storage errors and credentials.

## Open questions

- What default raw-report retention balances reprocessing and data minimization?
- Which S3-compatible implementations form the supported conformance matrix?
- When should direct multipart upload become necessary?
- Which backend checksum mechanisms are trustworthy enough to reduce full
  read-time verification?
