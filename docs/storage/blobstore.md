# BlobStore implementation

Open ASPM keeps raw report bytes behind the backend-neutral interface in
`internal/blobstore`. The initial adapters are:

- `filesystem` for local development and controlled single-node use;
- `s3store` for private S3-compatible object storage.

This implements the invariants accepted in
[ADR-0005](../adr/0005-raw-artifact-storage-and-retention.md). The ingestion
application service connects reserved imports to PostgreSQL artifact metadata
and this boundary. HTTP routing and authentication remain a separate delivery
slice.

## Immutable commit model

Both adapters use the same two-stage model:

1. stream bytes to a version-specific staging object while enforcing context,
   timeout, byte limit, optional expected size, and optional expected SHA-256;
2. publish a small manifest containing the server-generated key, exact size,
   SHA-256, opaque version, backend version when available, and commit time.

The manifest is the commit marker. Bytes without a manifest are invisible
orphans and may be cleaned after a grace period. A manifest is published only
after the byte stream and client expectations have been verified. Committed
bytes are never replaced in place.

The ingestion service stores a generated key before starting BlobStore I/O and
uses a fenced upload lease. If BlobStore publication succeeds but the metadata
transaction fails, a later full retry verifies its bytes against the committed
manifest and finishes the PostgreSQL transition. A conflicting retry never
replaces the committed object.

`Open` computes SHA-256 again while the caller consumes the stream. Callers must
read to EOF and close it; size or hash mismatch is returned as
`blobstore.ErrIntegrity` instead of successful EOF. The S3 adapter additionally
requests SDK/backend checksum verification.

## Keys and errors

Application code creates keys with `blobstore.NewKey`. Their 256 random bits are
encoded as an opaque `blb_...` value. `ParseKey` accepts only that exact format;
filenames, URLs, repository paths, workspace names, and client values cannot
become storage paths.

Backend errors are normalized. Returned errors never contain filesystem roots,
bucket names, endpoints, SDK responses, or credentials. Public handlers must
map the sentinel errors to their API contract instead of returning Go error
strings.

## Filesystem backend

The configured root must already exist, be absolute, and be different from the
filesystem root and current working directory. The adapter uses Go's bounded
`os.Root`, restrictive file modes, version-specific data files, regular-file
checks, and an atomic hard-link commit for manifests. Operators must protect the
root permissions and must not serve it through a web server.

## S3-compatible backend

The adapter receives an already configured AWS SDK v2 client. Credential
loading, endpoint selection, TLS trust, and credential rotation remain operator
concerns and are not copied into domain configuration or metadata.

Production buckets must be private, block public access, enforce TLS, enable
server-side encryption, and grant access only to the configured bucket/prefix.
`Config.Encryption` may request `AES256` or `aws:kms`; an empty value relies on
an enforced bucket default, which is also how the MinIO conformance test runs.

The transfer manager performs bounded multipart streaming for unknown lengths.
Every data object uses a version-specific key; the stable manifest is created
with `If-None-Match: *` to prevent concurrent replacement. S3 version IDs are
retained and used for guarded deletion when the backend supplies them.

## Verification

One conformance suite runs unchanged against both adapters. It covers streaming
content larger than internal buffers, write/read SHA-256, byte and time limits,
expected-digest mismatch, duplicate writes, guarded and idempotent delete, and
metadata consistency. Adapter-specific tests cover filesystem symlinks and
corruption plus S3 corruption against an isolated pinned MinIO image.

Run the filesystem suite with the ordinary checks:

```bash
make check
```

Run S3 conformance only against a disposable service:

```bash
OPEN_ASPM_TEST_S3_ENDPOINT=http://127.0.0.1:9000 \
OPEN_ASPM_TEST_S3_ACCESS_KEY=minioadmin \
OPEN_ASPM_TEST_S3_SECRET_KEY=minioadmin123 \
  make test-s3-integration
```

Never point integration tests at a shared or production bucket.
