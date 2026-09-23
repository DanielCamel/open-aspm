// Package jobqueue implements a PostgreSQL-backed, lease-fenced job queue.
package jobqueue

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

const (
	maxPayloadBytes = 60 << 10
	minLease        = time.Second
	maxLease        = time.Hour
)

var (
	ErrConflict              = errors.New("job idempotency conflict")
	ErrCancellationRequested = errors.New("job cancellation requested")
	ErrInvalid               = errors.New("invalid job queue input")
	ErrLeaseLost             = errors.New("job lease lost")
	ErrNotFound              = errors.New("job not found")
	ErrNotCancellable        = errors.New("job cannot be cancelled")
)

var namePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,62}$`)
var idempotencyKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
var capabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9._:-]{0,127}$`)

// State is the durable lifecycle state of a job.
type State string

const (
	StateQueued     State = "queued"
	StateLeased     State = "leased"
	StateRunning    State = "running"
	StateRetryWait  State = "retry_wait"
	StateSucceeded  State = "succeeded"
	StateDeadLetter State = "dead_letter"
	StateCancelled  State = "cancelled"
)

// ErrorClass controls whether a failed job may be attempted again.
type ErrorClass string

const (
	ErrorRetryable ErrorClass = "retryable"
	ErrorPermanent ErrorClass = "permanent"
)

// Spec describes a new immutable unit of work.
type Spec struct {
	WorkspaceID           string
	OperationID           string
	InitiatingPrincipalID string
	SystemCapability      string
	Queue                 string
	Kind                  string
	SchemaVersion         int
	Payload               json.RawMessage
	IdempotencyKey        string
	Priority              int
	AvailableAt           time.Time
	MaxAttempts           int
}

// Job is the safe queue record returned to a worker or operator.
type Job struct {
	ID                      string
	WorkspaceID             string
	OperationID             string
	InitiatingPrincipalID   string
	SystemCapability        string
	Queue                   string
	Kind                    string
	SchemaVersion           int
	Payload                 json.RawMessage
	IdempotencyKey          string
	State                   State
	Priority                int
	AvailableAt             time.Time
	AttemptCount            int
	MaxAttempts             int
	LeaseOwner              string
	LeaseExpiresAt          sql.NullTime
	HeartbeatAt             sql.NullTime
	CancellationRequestedAt sql.NullTime
	LastErrorClass          string
	LastErrorCode           string
	CreatedAt               time.Time
	StartedAt               sql.NullTime
	CompletedAt             sql.NullTime
	requestFingerprint      []byte
}

// Lease contains the current job plus its plaintext fencing token. The token
// is returned only to the worker that acquired it and is never stored at rest.
type Lease struct {
	Job
	token string
}

// FencingToken returns the sensitive token required for lease mutations. It
// is deliberately omitted from exported fields so generic serializers and
// structured loggers do not disclose it accidentally.
func (lease Lease) FencingToken() string {
	return lease.token
}

// String redacts the token when a lease is written through fmt or a logger
// that honors fmt.Stringer.
func (lease Lease) String() string {
	return fmt.Sprintf("Lease{JobID:%q WorkspaceID:%q FencingToken:[REDACTED]}", lease.ID, lease.WorkspaceID)
}

// GoString redacts the token for %#v formatting as well.
func (lease Lease) GoString() string {
	return lease.String()
}

// Failure is a bounded, safe failure classification. Detail and report data
// must not be stored in queue error fields.
type Failure struct {
	Class ErrorClass
	Code  string
}

// Metrics is a low-cardinality operational snapshot for one queue.
type Metrics struct {
	Queued              int64
	Running             int64
	RetryWait           int64
	DeadLetter          int64
	Succeeded           int64
	Cancelled           int64
	OldestQueuedAge     time.Duration
	AverageRunDuration  time.Duration
	ExpiredLeases       int64
	SaturatedWorkspaces int64
	Retries             int64
}

// Queue stores and leases jobs through PostgreSQL.
type Queue struct {
	db *sql.DB
}

// New returns a queue backed by an existing connection pool.
func New(db *sql.DB) (*Queue, error) {
	if db == nil {
		return nil, ErrInvalid
	}
	return &Queue{db: db}, nil
}

// Enqueue creates a job or returns the existing job for an identical
// idempotent request.
func (q *Queue) Enqueue(ctx context.Context, spec Spec) (Job, bool, error) {
	return q.enqueue(ctx, q.db, spec)
}

// EnqueueTx records queue intent inside a caller-owned transaction so domain
// state and its background work become visible atomically.
func (q *Queue) EnqueueTx(ctx context.Context, tx *sql.Tx, spec Spec) (Job, bool, error) {
	if tx == nil {
		return Job{}, false, ErrInvalid
	}
	return q.enqueue(ctx, tx, spec)
}

func (q *Queue) enqueue(ctx context.Context, executor sqlExecutor, spec Spec) (Job, bool, error) {
	canonicalPayload, fingerprint, err := fingerprintSpec(spec)
	if err != nil {
		return Job{}, false, err
	}
	spec.Payload = canonicalPayload
	var availableAt any
	if spec.AvailableAt.IsZero() {
		availableAt = nil
	} else {
		availableAt = spec.AvailableAt.UTC()
	}
	id, err := randomID("job_", 20)
	if err != nil {
		return Job{}, false, err
	}

	result, err := executor.ExecContext(ctx, `
		INSERT INTO open_aspm.jobs (
			id, workspace_id, operation_id, initiating_principal_id, system_capability,
			queue, kind, schema_version,
			payload, idempotency_key, request_fingerprint, priority, available_at, max_attempts
		) VALUES (
			$1, $2, NULLIF($3, ''), $4, $5, $6, $7, $8, $9, $10, $11, $12,
			COALESCE($13, clock_timestamp()), $14
		)
		ON CONFLICT (workspace_id, queue, kind, idempotency_key) DO NOTHING`,
		id, spec.WorkspaceID, spec.OperationID, spec.InitiatingPrincipalID, spec.SystemCapability,
		spec.Queue, spec.Kind, spec.SchemaVersion, []byte(spec.Payload), spec.IdempotencyKey,
		fingerprint, spec.Priority, availableAt, spec.MaxAttempts,
	)
	if err != nil {
		return Job{}, false, fmt.Errorf("enqueue job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, fmt.Errorf("read enqueue result: %w", err)
	}
	job, err := q.byIdempotencyKey(ctx, executor, spec)
	if err != nil {
		return Job{}, false, err
	}
	created := rows == 1
	if !created && !bytes.Equal(job.requestFingerprint, fingerprint) {
		return Job{}, false, ErrConflict
	}
	return job, created, nil
}

// Acquire leases one eligible job. The boolean is false when the queue has no
// available work. Fairness prefers workspaces with fewer active jobs.
func (q *Queue) Acquire(
	ctx context.Context,
	queue string,
	owner string,
	leaseDuration time.Duration,
	maxInFlightPerWorkspace int,
) (Lease, bool, error) {
	if !validName(queue) || strings.TrimSpace(owner) != owner || owner == "" ||
		strings.IndexByte(owner, 0) >= 0 || len(owner) > 128 ||
		leaseDuration < minLease || leaseDuration > maxLease ||
		maxInFlightPerWorkspace < 1 || maxInFlightPerWorkspace > 100 {
		return Lease{}, false, ErrInvalid
	}
	token, err := randomID("lease_", 32)
	if err != nil {
		return Lease{}, false, err
	}
	tokenHash := hashLeaseToken(token)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return Lease{}, false, fmt.Errorf("begin lease: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		"open-aspm-job-acquire:"+queue,
	); err != nil {
		return Lease{}, false, fmt.Errorf("lock job acquisition: %w", err)
	}

	row := tx.QueryRowContext(ctx, `
		WITH candidate AS (
			SELECT job.id
			FROM open_aspm.jobs AS job
			WHERE job.queue = $1
			  AND job.state IN ('queued', 'retry_wait')
			  AND job.available_at <= now()
			  AND job.cancellation_requested_at IS NULL
			  AND (
				SELECT count(*)
				FROM open_aspm.jobs AS limited
				WHERE limited.workspace_id = job.workspace_id
				  AND limited.queue = job.queue
				  AND limited.state IN ('leased', 'running')
			  ) < $5
			ORDER BY (
				SELECT count(*)
				FROM open_aspm.jobs AS active
				WHERE active.workspace_id = job.workspace_id
				  AND active.queue = job.queue
				  AND active.state IN ('leased', 'running')
			) ASC, job.priority DESC, job.available_at ASC, job.created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE open_aspm.jobs AS job
		SET state = 'leased',
			attempt_count = attempt_count + 1,
			lease_owner = $2,
			lease_token_hash = $3,
			lease_expires_at = now() + ($4 * interval '1 microsecond'),
			heartbeat_at = now()
		FROM candidate
		WHERE job.id = candidate.id
		RETURNING `+jobColumns("job"), queue, owner, tokenHash, leaseDuration.Microseconds(), maxInFlightPerWorkspace)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, false, nil
	}
	if err != nil {
		return Lease{}, false, fmt.Errorf("acquire job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.job_attempts (
			job_id, workspace_id, attempt_number, lease_owner, leased_at, last_heartbeat_at
		) VALUES ($1, $2, $3, $4, now(), now())`,
		job.ID, job.WorkspaceID, job.AttemptCount, owner,
	); err != nil {
		return Lease{}, false, fmt.Errorf("record job attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, false, fmt.Errorf("commit lease: %w", err)
	}
	return Lease{Job: job, token: token}, true, nil
}

// Start marks a leased job as running. The lease token fences stale workers.
func (q *Queue) Start(ctx context.Context, workspaceID, id, token string) error {
	if err := validateLeaseIdentity(workspaceID, id, token); err != nil {
		return err
	}
	tokenHash := hashLeaseToken(token)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job start: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.jobs
		SET state = 'running', started_at = COALESCE(started_at, now())
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3 AND state = 'leased'
		  AND lease_expires_at > now() AND cancellation_requested_at IS NULL`, workspaceID, id, tokenHash)
	if err != nil {
		return fmt.Errorf("start job: %w", err)
	}
	if err := requireLease(result); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return classifyLeaseMiss(ctx, tx, workspaceID, id, tokenHash)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts AS attempt
		SET started_at = COALESCE(started_at, now())
		FROM open_aspm.jobs AS job
		WHERE job.workspace_id = $1 AND job.id = $2 AND job.lease_token_hash = $3
		  AND attempt.workspace_id = job.workspace_id
		  AND attempt.job_id = job.id AND attempt.attempt_number = job.attempt_count`, workspaceID, id, tokenHash); err != nil {
		return fmt.Errorf("record job start: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job start: %w", err)
	}
	return nil
}

// Heartbeat extends a current lease.
func (q *Queue) Heartbeat(
	ctx context.Context,
	workspaceID, id, token string,
	leaseDuration time.Duration,
) error {
	if leaseDuration < minLease || leaseDuration > maxLease {
		return ErrInvalid
	}
	if err := validateLeaseIdentity(workspaceID, id, token); err != nil {
		return err
	}
	tokenHash := hashLeaseToken(token)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin heartbeat: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.jobs
		SET heartbeat_at = now(), lease_expires_at = now() + ($4 * interval '1 microsecond')
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3 AND state IN ('leased', 'running')
		  AND lease_expires_at > now() AND cancellation_requested_at IS NULL`,
		workspaceID, id, tokenHash, leaseDuration.Microseconds())
	if err != nil {
		return fmt.Errorf("heartbeat job: %w", err)
	}
	if err := requireLease(result); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return classifyLeaseMiss(ctx, tx, workspaceID, id, tokenHash)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts AS attempt
		SET last_heartbeat_at = now()
		FROM open_aspm.jobs AS job
		WHERE job.workspace_id = $1 AND job.id = $2 AND job.lease_token_hash = $3
		  AND attempt.workspace_id = job.workspace_id
		  AND attempt.job_id = job.id AND attempt.attempt_number = job.attempt_count`, workspaceID, id, tokenHash); err != nil {
		return fmt.Errorf("record heartbeat: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit heartbeat: %w", err)
	}
	return nil
}

// Succeed completes a running job.
func (q *Queue) Succeed(ctx context.Context, workspaceID, id, token string) error {
	return q.finish(ctx, workspaceID, id, token, StateSucceeded)
}

// SucceedTx acknowledges a lease inside a caller-owned transaction. An
// idempotent database-only handler can commit its domain result and queue
// acknowledgement without a crash window between two transactions.
func (q *Queue) SucceedTx(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID, id, token string,
) error {
	if tx == nil {
		return ErrInvalid
	}
	return q.finishTx(ctx, tx, workspaceID, id, token, StateSucceeded)
}

// Fail records a safe failure. Retryable failures wait until availableAt;
// permanent or exhausted failures move to dead-letter state.
func (q *Queue) Fail(
	ctx context.Context,
	workspaceID string,
	id string,
	token string,
	failure Failure,
	availableAt time.Time,
) (State, error) {
	if (failure.Class != ErrorRetryable && failure.Class != ErrorPermanent) || !validCode(failure.Code) {
		return "", ErrInvalid
	}
	if err := validateLeaseIdentity(workspaceID, id, token); err != nil {
		return "", err
	}
	tokenHash := hashLeaseToken(token)
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin job failure: %w", err)
	}
	defer tx.Rollback()

	var attemptCount, maxAttempts int
	var cancellationRequested bool
	err = tx.QueryRowContext(ctx, `
		SELECT attempt_count, max_attempts, cancellation_requested_at IS NOT NULL
		FROM open_aspm.jobs
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3 AND state = 'running'
		  AND lease_expires_at > now()
		FOR UPDATE`, workspaceID, id, tokenHash).Scan(&attemptCount, &maxAttempts, &cancellationRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrLeaseLost
	}
	if err != nil {
		return "", fmt.Errorf("lock failed job: %w", err)
	}
	state := StateDeadLetter
	outcome := "dead_letter"
	if cancellationRequested {
		state = StateCancelled
		outcome = "cancelled"
	} else if failure.Class == ErrorRetryable && attemptCount < maxAttempts {
		state = StateRetryWait
		outcome = "retry"
		if availableAt.IsZero() {
			return "", ErrInvalid
		}
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.jobs
		SET state = $4, available_at = CASE WHEN $4 = 'retry_wait' THEN $5 ELSE available_at END,
			lease_owner = NULL, lease_token_hash = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
			last_error_class = CASE WHEN $4 = 'cancelled' THEN last_error_class ELSE $6 END,
			last_error_code = CASE WHEN $4 = 'cancelled' THEN last_error_code ELSE $7 END,
			completed_at = CASE WHEN $4 IN ('dead_letter', 'cancelled') THEN now() ELSE NULL END
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3`,
		workspaceID, id, tokenHash, state, availableAt, failure.Class, failure.Code); err != nil {
		return "", fmt.Errorf("fail job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts
		SET finished_at = now(), outcome = $4,
			safe_error_code = CASE WHEN $4 = 'cancelled' THEN NULL ELSE $5 END
		WHERE workspace_id = $1 AND job_id = $2 AND attempt_number = $3`,
		workspaceID, id, attemptCount, outcome, failure.Code); err != nil {
		return "", fmt.Errorf("finish failed attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit job failure: %w", err)
	}
	return state, nil
}

// RequestCancellation durably records cancellation intent. Jobs that are not
// active are cancelled immediately.
func (q *Queue) RequestCancellation(ctx context.Context, workspaceID, id string) (State, error) {
	if err := validateResourceIdentity(workspaceID, id); err != nil {
		return "", err
	}
	var state State
	err := q.db.QueryRowContext(ctx, `
		UPDATE open_aspm.jobs
		SET cancellation_requested_at = COALESCE(cancellation_requested_at, now()),
			state = CASE WHEN state IN ('queued', 'retry_wait') THEN 'cancelled' ELSE state END,
			completed_at = CASE WHEN state IN ('queued', 'retry_wait') THEN now() ELSE completed_at END
		WHERE workspace_id = $1 AND id = $2 AND state NOT IN ('succeeded', 'dead_letter', 'cancelled')
		RETURNING state`, workspaceID, id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if scanErr := q.db.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM open_aspm.jobs WHERE workspace_id = $1 AND id = $2)`,
			workspaceID, id,
		).Scan(&exists); scanErr != nil {
			return "", fmt.Errorf("check cancellation target: %w", scanErr)
		}
		if !exists {
			return "", ErrNotFound
		}
		return "", ErrNotCancellable
	}
	if err != nil {
		return "", fmt.Errorf("request job cancellation: %w", err)
	}
	return state, nil
}

// CancelActive acknowledges cancellation from the current worker.
func (q *Queue) CancelActive(ctx context.Context, workspaceID, id, token string) error {
	return q.finish(ctx, workspaceID, id, token, StateCancelled)
}

// RecoverExpired reclaims expired leases in a bounded batch. Running jobs wait
// for retryDelay before they can be acquired again.
func (q *Queue) RecoverExpired(
	ctx context.Context,
	queue string,
	limit int,
	retryDelay time.Duration,
) (int, error) {
	if !validName(queue) || limit <= 0 || limit > 1000 || retryDelay < 0 {
		return 0, ErrInvalid
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin lease recovery: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		WITH ranked AS (
			SELECT workspace_id, id,
				row_number() OVER (
					PARTITION BY workspace_id ORDER BY lease_expires_at, id
				) AS workspace_rank
			FROM open_aspm.jobs
			WHERE queue = $1 AND state IN ('leased', 'running') AND lease_expires_at <= now()
		)
		SELECT job.workspace_id, job.id, job.state, job.attempt_count, job.max_attempts,
		       job.cancellation_requested_at IS NOT NULL
		FROM open_aspm.jobs AS job
		JOIN ranked USING (workspace_id, id)
		ORDER BY ranked.workspace_rank, job.lease_expires_at, job.workspace_id, job.id
		FOR UPDATE OF job SKIP LOCKED
		LIMIT $2`, queue, limit)
	if err != nil {
		return 0, fmt.Errorf("select expired leases: %w", err)
	}
	type expired struct {
		workspace string
		id        string
		state     State
		attempt   int
		max       int
		cancelled bool
	}
	var jobs []expired
	for rows.Next() {
		var job expired
		if err := rows.Scan(&job.workspace, &job.id, &job.state, &job.attempt, &job.max, &job.cancelled); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan expired lease: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, fmt.Errorf("iterate expired leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close expired leases: %w", err)
	}
	for _, job := range jobs {
		state := StateQueued
		outcome := "lease_expired"
		completed := false
		var availableAt time.Time
		if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&availableAt); err != nil {
			return 0, fmt.Errorf("read database time for lease recovery: %w", err)
		}
		if job.cancelled {
			state, outcome, completed = StateCancelled, "cancelled", true
		} else if job.attempt >= job.max {
			state, outcome, completed = StateDeadLetter, "dead_letter", true
		} else if job.state == StateRunning {
			state = StateRetryWait
			availableAt = availableAt.Add(retryDelay)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_aspm.jobs
			SET state = $3, available_at = $4,
				lease_owner = NULL, lease_token_hash = NULL, lease_expires_at = NULL, heartbeat_at = NULL,
				last_error_class = CASE
					WHEN $3 = 'cancelled' THEN last_error_class
					WHEN $3 = 'dead_letter' THEN 'worker'
					ELSE 'retryable'
				END,
				last_error_code = CASE WHEN $3 = 'cancelled' THEN last_error_code ELSE 'lease_expired' END,
				completed_at = CASE WHEN $5 THEN now() ELSE NULL END
			WHERE workspace_id = $1 AND id = $2`,
			job.workspace, job.id, state, availableAt, completed,
		); err != nil {
			return 0, fmt.Errorf("recover expired lease: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_aspm.job_attempts
			SET finished_at = now(), outcome = $4,
				safe_error_code = CASE WHEN $4 = 'cancelled' THEN NULL ELSE 'lease_expired' END
			WHERE workspace_id = $1 AND job_id = $2 AND attempt_number = $3`,
			job.workspace, job.id, job.attempt, outcome,
		); err != nil {
			return 0, fmt.Errorf("finish expired attempt: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit lease recovery: %w", err)
	}
	return len(jobs), nil
}

// Metrics returns bounded aggregate data suitable for telemetry instruments.
func (q *Queue) Metrics(ctx context.Context, queue string, maxInFlightPerWorkspace int) (Metrics, error) {
	if !validName(queue) || maxInFlightPerWorkspace < 1 || maxInFlightPerWorkspace > 100 {
		return Metrics{}, ErrInvalid
	}
	var metrics Metrics
	var oldestSeconds, averageSeconds float64
	err := q.db.QueryRowContext(ctx, `
		SELECT
			count(*) FILTER (WHERE state = 'queued'),
			count(*) FILTER (WHERE state IN ('leased', 'running')),
			count(*) FILTER (WHERE state = 'retry_wait'),
			count(*) FILTER (WHERE state = 'dead_letter'),
			count(*) FILTER (WHERE state = 'succeeded'),
			count(*) FILTER (WHERE state = 'cancelled'),
			COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE state = 'queued')), 0),
			COALESCE((
				SELECT avg(EXTRACT(EPOCH FROM attempt.finished_at - attempt.started_at))
				FROM open_aspm.job_attempts AS attempt
				JOIN open_aspm.jobs AS measured_job
				  ON measured_job.workspace_id = attempt.workspace_id
				 AND measured_job.id = attempt.job_id
				WHERE measured_job.queue = $1
				  AND attempt.finished_at IS NOT NULL
				  AND attempt.started_at IS NOT NULL
			), 0),
			count(*) FILTER (WHERE state IN ('leased', 'running') AND lease_expires_at <= now()),
			(
				SELECT count(*)
				FROM (
					SELECT workspace_id
					FROM open_aspm.jobs AS active
					WHERE active.queue = $1 AND active.state IN ('leased', 'running')
					GROUP BY workspace_id
					HAVING count(*) >= $2
				) AS saturated
			),
			(
				SELECT count(*)
				FROM open_aspm.job_attempts AS retry_attempt
				JOIN open_aspm.jobs AS retried_job
				  ON retried_job.workspace_id = retry_attempt.workspace_id
				 AND retried_job.id = retry_attempt.job_id
				WHERE retried_job.queue = $1 AND retry_attempt.attempt_number > 1
			)
		FROM open_aspm.jobs
		WHERE queue = $1`, queue, maxInFlightPerWorkspace).Scan(
		&metrics.Queued, &metrics.Running, &metrics.RetryWait,
		&metrics.DeadLetter, &metrics.Succeeded, &metrics.Cancelled,
		&oldestSeconds, &averageSeconds, &metrics.ExpiredLeases,
		&metrics.SaturatedWorkspaces, &metrics.Retries,
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("read queue metrics: %w", err)
	}
	metrics.OldestQueuedAge = time.Duration(oldestSeconds * float64(time.Second))
	metrics.AverageRunDuration = time.Duration(averageSeconds * float64(time.Second))
	return metrics, nil
}

func (q *Queue) finish(ctx context.Context, workspaceID, id, token string, state State) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job completion: %w", err)
	}
	defer tx.Rollback()
	if err := q.finishTx(ctx, tx, workspaceID, id, token, state); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job completion: %w", err)
	}
	return nil
}

func (q *Queue) finishTx(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID, id, token string,
	state State,
) error {
	if state != StateSucceeded && state != StateCancelled {
		return ErrInvalid
	}
	if err := validateLeaseIdentity(workspaceID, id, token); err != nil {
		return err
	}
	tokenHash := hashLeaseToken(token)
	var attempt int
	err := tx.QueryRowContext(ctx, `
		UPDATE open_aspm.jobs
		SET state = $4, lease_owner = NULL, lease_token_hash = NULL,
			lease_expires_at = NULL, heartbeat_at = NULL, completed_at = now()
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3
		  AND (state = 'running' OR ($4 = 'cancelled' AND state = 'leased'))
		  AND lease_expires_at > now()
		  AND ($4 = 'cancelled' OR cancellation_requested_at IS NULL)
		RETURNING attempt_count`, workspaceID, id, tokenHash, state).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		if state == StateSucceeded {
			return classifyLeaseMiss(ctx, tx, workspaceID, id, tokenHash)
		}
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts
		SET finished_at = now(), outcome = $4
		WHERE workspace_id = $1 AND job_id = $2 AND attempt_number = $3`,
		workspaceID, id, attempt, state,
	); err != nil {
		return fmt.Errorf("finish job attempt: %w", err)
	}
	return nil
}

func (q *Queue) byIdempotencyKey(ctx context.Context, executor sqlExecutor, spec Spec) (Job, error) {
	row := executor.QueryRowContext(ctx, `
		SELECT `+jobColumns("job")+`
		FROM open_aspm.jobs AS job
		WHERE workspace_id = $1 AND queue = $2 AND kind = $3 AND idempotency_key = $4`,
		spec.WorkspaceID, spec.Queue, spec.Kind, spec.IdempotencyKey)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("read enqueued job: %w", err)
	}
	return job, nil
}

type scanner interface {
	Scan(...any) error
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func scanJob(row scanner) (Job, error) {
	var job Job
	var operationID, leaseOwner sql.NullString
	var lastErrorClass, lastErrorCode sql.NullString
	err := row.Scan(
		&job.ID, &job.WorkspaceID, &operationID,
		&job.InitiatingPrincipalID, &job.SystemCapability, &job.Queue, &job.Kind,
		&job.SchemaVersion, &job.Payload, &job.IdempotencyKey, &job.requestFingerprint, &job.State,
		&job.Priority, &job.AvailableAt, &job.AttemptCount, &job.MaxAttempts,
		&leaseOwner, &job.LeaseExpiresAt, &job.HeartbeatAt,
		&job.CancellationRequestedAt, &lastErrorClass, &lastErrorCode,
		&job.CreatedAt, &job.StartedAt, &job.CompletedAt,
	)
	if err != nil {
		return Job{}, err
	}
	job.OperationID = operationID.String
	job.LeaseOwner = leaseOwner.String
	job.LastErrorClass = lastErrorClass.String
	job.LastErrorCode = lastErrorCode.String
	return job, nil
}

func jobColumns(alias string) string {
	return strings.Join([]string{
		alias + ".id", alias + ".workspace_id", alias + ".operation_id",
		alias + ".initiating_principal_id", alias + ".system_capability",
		alias + ".queue", alias + ".kind", alias + ".schema_version",
		alias + ".payload", alias + ".idempotency_key", alias + ".request_fingerprint", alias + ".state",
		alias + ".priority", alias + ".available_at", alias + ".attempt_count",
		alias + ".max_attempts", alias + ".lease_owner",
		alias + ".lease_expires_at", alias + ".heartbeat_at",
		alias + ".cancellation_requested_at", alias + ".last_error_class",
		alias + ".last_error_code", alias + ".created_at", alias + ".started_at",
		alias + ".completed_at",
	}, ", ")
}

func validateSpec(spec Spec) error {
	if validateOpaqueID(spec.WorkspaceID) != nil ||
		(spec.OperationID != "" && validateOpaqueID(spec.OperationID) != nil) ||
		validateOpaqueID(spec.InitiatingPrincipalID) != nil ||
		!capabilityPattern.MatchString(spec.SystemCapability) || !validName(spec.Queue) ||
		!validName(spec.Kind) || spec.SchemaVersion <= 0 ||
		!idempotencyKeyPattern.MatchString(spec.IdempotencyKey) ||
		spec.Priority < -1000 || spec.Priority > 1000 ||
		spec.MaxAttempts <= 0 || spec.MaxAttempts > 100 ||
		len(spec.Payload) == 0 || len(spec.Payload) > maxPayloadBytes || !json.Valid(spec.Payload) {
		return ErrInvalid
	}
	return nil
}

func fingerprintSpec(spec Spec) ([]byte, []byte, error) {
	if err := validateSpec(spec); err != nil {
		return nil, nil, err
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(spec.Payload))
	decoder.UseNumber()
	if decoder.Decode(&payload) != nil || payload == nil {
		return nil, nil, ErrInvalid
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return nil, nil, ErrInvalid
	}
	canonicalPayload, err := json.Marshal(payload)
	if err != nil || len(canonicalPayload) > maxPayloadBytes {
		return nil, nil, ErrInvalid
	}
	availableAt := ""
	if !spec.AvailableAt.IsZero() {
		availableAt = spec.AvailableAt.UTC().Format(time.RFC3339Nano)
	}
	input := struct {
		WorkspaceID           string          `json:"workspace_id"`
		OperationID           string          `json:"operation_id,omitempty"`
		InitiatingPrincipalID string          `json:"initiating_principal_id"`
		SystemCapability      string          `json:"system_capability"`
		Queue                 string          `json:"queue"`
		Kind                  string          `json:"kind"`
		SchemaVersion         int             `json:"schema_version"`
		Payload               json.RawMessage `json:"payload"`
		Priority              int             `json:"priority"`
		AvailableAt           string          `json:"available_at"`
		MaxAttempts           int             `json:"max_attempts"`
	}{
		WorkspaceID: spec.WorkspaceID, OperationID: spec.OperationID,
		InitiatingPrincipalID: spec.InitiatingPrincipalID,
		SystemCapability:      spec.SystemCapability,
		Queue:                 spec.Queue, Kind: spec.Kind, SchemaVersion: spec.SchemaVersion,
		Payload: canonicalPayload, Priority: spec.Priority,
		AvailableAt: availableAt, MaxAttempts: spec.MaxAttempts,
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, nil, fmt.Errorf("fingerprint job request: %w", err)
	}
	fingerprint := sha256.Sum256(encoded)
	return canonicalPayload, fingerprint[:], nil
}

func sameSpec(job Job, spec Spec) bool {
	var existing, requested any
	existingDecoder := json.NewDecoder(bytes.NewReader(job.Payload))
	existingDecoder.UseNumber()
	requestedDecoder := json.NewDecoder(bytes.NewReader(spec.Payload))
	requestedDecoder.UseNumber()
	if existingDecoder.Decode(&existing) != nil || requestedDecoder.Decode(&requested) != nil {
		return false
	}
	existingJSON, _ := json.Marshal(existing)
	requestedJSON, _ := json.Marshal(requested)
	return job.OperationID == spec.OperationID &&
		job.InitiatingPrincipalID == spec.InitiatingPrincipalID &&
		job.SystemCapability == spec.SystemCapability &&
		job.SchemaVersion == spec.SchemaVersion &&
		string(existingJSON) == string(requestedJSON) &&
		job.Priority == spec.Priority && job.MaxAttempts == spec.MaxAttempts
}

func validName(value string) bool {
	return namePattern.MatchString(value)
}

func validCode(value string) bool {
	return value != "" && len(value) <= 128 && namePattern.MatchString(value)
}

func validateOpaqueID(value string) error {
	if len(value) < 3 || len(value) > 128 || strings.TrimSpace(value) != value || strings.IndexByte(value, 0) >= 0 {
		return ErrInvalid
	}
	return nil
}

func validateResourceIdentity(workspaceID, id string) error {
	if validateOpaqueID(workspaceID) != nil || validateOpaqueID(id) != nil {
		return ErrInvalid
	}
	return nil
}

func validateLeaseIdentity(workspaceID, id, token string) error {
	if validateResourceIdentity(workspaceID, id) != nil ||
		!strings.HasPrefix(token, "lease_") || len(token) != len("lease_")+52 {
		return ErrInvalid
	}
	return nil
}

func hashLeaseToken(token string) []byte {
	digest := sha256.Sum256([]byte(token))
	return digest[:]
}

func requireLease(result sql.Result) error {
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read lease result: %w", err)
	}
	if rows != 1 {
		return ErrLeaseLost
	}
	return nil
}

func classifyLeaseMiss(
	ctx context.Context,
	tx *sql.Tx,
	workspaceID, id string,
	tokenHash []byte,
) error {
	var cancellationRequested bool
	err := tx.QueryRowContext(ctx, `
		SELECT cancellation_requested_at IS NOT NULL
		FROM open_aspm.jobs
		WHERE workspace_id = $1 AND id = $2 AND lease_token_hash = $3
		  AND state IN ('leased', 'running') AND lease_expires_at > now()`,
		workspaceID, id, tokenHash,
	).Scan(&cancellationRequested)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("classify lease loss: %w", err)
	}
	if cancellationRequested {
		return ErrCancellationRequested
	}
	return ErrLeaseLost
}

func randomID(prefix string, byteCount int) (string, error) {
	value := make([]byte, byteCount)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate queue identifier: %w", err)
	}
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(value)
	return prefix + strings.ToLower(encoded), nil
}
