# ADR-0003: PostgreSQL job leasing and retry semantics

- **Status:** Accepted
- **Date:** 2026-09-22
- **Owners:** Open ASPM maintainers
- **Related issues:** [#6](https://github.com/spectremi/open-aspm/issues/6)
- **Supersedes:** None
- **Superseded by:** None

## Context

Report parsing, normalization, correlation, enrichment, projection updates, and
connector synchronization can exceed an HTTP request lifetime and require
retries. The first Open ASPM deployment should remain simple and must not
require a separate message broker.

PostgreSQL can provide durable work leasing, but only if crash recovery,
concurrent workers, stale completion, retries, cancellation, idempotency, and
workspace fairness are defined explicitly.

This queue provides at-least-once execution. Exactly-once external side effects
cannot be guaranteed by a lease table alone.

## Decision

### 1. PostgreSQL is the initial authoritative job queue

Jobs are stored in PostgreSQL and leased using short transactions with row
locking and `FOR UPDATE SKIP LOCKED`. A worker does not hold a database
transaction open while executing a job.

The queue is an internal infrastructure contract. Public clients observe an
`Operation`, not queue rows or lease mechanics.

### 2. Job states are explicit

```text
queued -> leased -> running -> succeeded
   ^         |          |
   |         |          +-> retry_wait -> queued
   |         |          +-> dead_letter
   |         |          +-> cancelled
   |         +------------> queued       (expired before start)
   +---------------------- retry schedule
```

Terminal states are:

```text
succeeded
dead_letter
cancelled
```

`leased` and `running` are distinct for observability. A worker may mark a job
running immediately after successfully obtaining and validating the lease.

### 3. Minimum job record

```text
Job
  id
  workspace_id
  operation_id?
  queue
  kind
  schema_version
  payload
  idempotency_key
  state
  priority
  available_at
  attempt_count
  max_attempts
  lease_owner?
  lease_token?
  lease_expires_at?
  heartbeat_at?
  cancellation_requested_at?
  last_error_class?
  last_error_code?
  created_at
  started_at?
  completed_at?
```

Payloads contain identifiers and bounded metadata, not raw reports or
credentials. Workers retrieve large content and secrets through authorized
storage interfaces.

### 4. Lease acquisition is short and fenced

A worker acquires a bounded batch of eligible jobs in one short transaction:

1. select `queued` jobs whose `available_at` has passed, respecting queue,
   workspace fairness, and priority;
2. lock candidates with `FOR UPDATE SKIP LOCKED`;
3. assign a cryptographically random or otherwise unguessable `lease_token`;
4. record owner, expiry, heartbeat, and increment the attempt count; and
5. commit before processing begins.

Every heartbeat, completion, retry, and failure update must match both job ID
and the current lease token. A worker holding an expired or superseded token is
fenced and cannot commit job state.

Domain writes produced by a job additionally use idempotency constraints. A
lease token prevents stale queue completion; it cannot by itself undo external
effects created before lease loss.

### 5. Leases expire and are recoverable

Lease duration is configurable per job kind within operator-defined bounds.
Workers heartbeat substantially before expiry. Heartbeats extend leases only
for the current token and never beyond a configured maximum without continued
progress.

A reaper or lease-acquisition path detects expiry:

- an expired `leased` job returns to `queued`;
- an expired `running` job becomes retryable or dead-lettered according to its
  attempt policy;
- cancellation requested before recovery produces `cancelled` if no committed
  domain result exists.

Recovery records an attempt event. It never silently resets attempt history.

### 6. Retry classification is typed

Handlers return a structured outcome:

```text
success
retryable(code, safe_message, optional_retry_after)
permanent(code, safe_message)
cancelled
```

Examples of retryable failure include temporary database unavailability,
provider rate limiting, and transient object-store failure. Examples of
permanent failure include unsupported format, invalid schema, exceeded content
limit, and revoked authorization that requires operator action.

Unknown panics or process termination consume an attempt after lease recovery.
Error details are redacted before persistence and telemetry.

### 7. Backoff is bounded and jittered

Retry scheduling uses exponential backoff with jitter, bounded by per-kind
minimum, maximum, attempt count, and total retry duration. A provider-supplied
safe `Retry-After` may increase the delay within operator bounds.

Retries stop when any of these conditions is reached:

- `max_attempts`;
- maximum total retry age;
- operation cancellation;
- a permanent failure; or
- an administrative dead-letter action.

### 8. Handlers are idempotent

Every job kind defines:

- an idempotency scope and key;
- authoritative inputs;
- database uniqueness constraints;
- permitted external side effects;
- replay behavior after partial completion.

For database-only work, domain changes and job success should be committed in
one transaction where practical. Otherwise, a durable result or checkpoint is
written before acknowledgement.

External mutations use provider idempotency support when available. When it is
not available, Open ASPM stores an external-operation record and reconciliation
state; it does not claim exactly-once delivery.

### 9. Cancellation is cooperative and durable

Cancellation records intent before worker interruption:

- queued and retry-wait jobs can transition directly to `cancelled`;
- leased or running jobs receive `cancellation_requested_at`;
- workers check cancellation between bounded processing stages;
- committed domain results are not rolled back by relabeling a completed job as
  cancelled.

Operation status distinguishes cancellation requested, cancelled before side
effects, and completed despite a late cancellation request.

### 10. Dead-letter jobs remain inspectable

Dead-letter records preserve job kind, safe error classification, attempt
history, identifiers, and timestamps. Raw credentials and unbounded payloads
are not copied into error records.

Authorized operators may:

- inspect safe diagnostic metadata;
- retry after correcting configuration;
- cancel permanently; or
- reference the job in a support export with explicit redaction.

Manual retry creates a new attempt under the same job identity or a documented
replacement relationship; it does not erase failure history.

### 11. Workspace fairness is a queue invariant

Acquisition must not select only the globally oldest or highest-priority jobs
without workspace limits. The initial implementation uses bounded per-workspace
in-flight concurrency and fair candidate selection appropriate to PostgreSQL.

Operator-defined system jobs may use a reserved queue, but workspace-controlled
priority cannot starve security, cleanup, or lease-recovery work.

### 12. Jobs emit append-only attempt history

The mutable job row represents current state. An append-only `JobAttempt` or
equivalent event record captures:

```text
job_id
attempt_number
lease_owner
started_at
last_heartbeat_at
finished_at
outcome
safe_error_code
```

This record supports diagnosis without turning the job queue into a general
domain event store.

## Worker termination behavior

| Termination point | Expected recovery |
| --- | --- |
| Before lease transaction commits | Another worker may acquire the unchanged job |
| After lease commit, before handler starts | Lease expires and job is retried |
| During a database-only idempotent handler | Transaction rolls back or replay observes committed idempotency record |
| After domain commit, before job success | Replay detects the durable result and marks success without duplication |
| During external provider call | Reconciliation uses provider idempotency key or external-operation record |
| After success transaction commits | Job remains succeeded and cannot be leased again |

## Alternatives considered

### Require Kafka, NATS, or another broker initially

Not selected because it adds operational dependencies before workload and
delivery requirements are measured. The application-level job contract should
permit a future adapter if PostgreSQL becomes a demonstrated constraint.

### Execute all work inside HTTP requests

Rejected because long parsing and integration calls would couple client
timeouts to processing and make retry and crash recovery unreliable.

### Hold a row lock for the entire job

Rejected because long transactions increase contention, complicate failover,
and do not provide safe handling of external effects.

### Assume exactly-once execution

Rejected because process failure can occur between a side effect and queue
acknowledgement. Idempotency and reconciliation are explicit instead.

### Use an in-memory queue

Rejected for authoritative work because restart would lose jobs and state.

## Consequences

### Positive

- The initial deployment requires fewer services.
- Job and domain transactions can share PostgreSQL consistency where useful.
- Crash, retry, cancellation, and dead-letter behavior is explicit.
- The public operation model is independent of the queue implementation.

### Negative and trade-offs

- Fair scheduling and cleanup add database logic.
- High-throughput workloads may eventually exceed comfortable PostgreSQL queue
  behavior.
- External side effects still require provider-specific reconciliation.
- Operators must monitor table growth, vacuum behavior, queue age, and expired
  leases.

## Security and privacy impact

Job payloads and errors can become an indirect secret store. Payload schemas
therefore prohibit credentials and raw report bodies. Workspace authorization
applies to operation and dead-letter inspection.

Lease fencing protects integrity against stale workers. Workspace concurrency
limits and bounded retry reduce denial-of-service impact. Worker identities and
job kinds are included in audit and telemetry, while lease tokens are treated
as sensitive and are not logged.

## Compatibility and migration

Job payloads carry a schema version. A rolling upgrade must either process the
previous supported payload version or delay producers until compatible workers
are available.

If a future broker is introduced, public operation IDs and job-kind semantics
remain stable. Queue-specific lease rows are not exposed through public APIs.

## Verification

Integration tests must exercise concurrent acquisition, lease fencing, expiry,
heartbeat, retry delay, jitter bounds, cancellation, dead-letter transition,
workspace fairness, and worker termination at each point in the recovery table.

Tests use real PostgreSQL behavior rather than replacing locking semantics with
an in-memory mock. Metrics verify queue depth, oldest age, execution duration,
attempt count, lease expiry, retry count, dead-letter count, and workspace
saturation.

## Open questions

- Which fair-selection query provides the best initial balance of clarity and
  PostgreSQL performance?
- Should long parsing jobs support durable internal checkpoints in v0.1?
- What default attempt and retry-age limits apply to each initial job kind?
- Which administrative dead-letter operations belong in the first UI?
