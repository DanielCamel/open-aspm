// Package jobqueue implements a PostgreSQL-backed, lease-fenced job queue.
package jobqueue

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

const (
	maxPayloadBytes = 64 << 10
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
	WorkspaceID    string
	OperationID    string
	Queue          string
	Kind           string
	SchemaVersion  int
	Payload        json.RawMessage
	IdempotencyKey string
	Priority       int
	AvailableAt    time.Time
	MaxAttempts    int
}

// Job is the safe queue record returned to a worker or operator.
type Job struct {
	ID                      string
	WorkspaceID             string
	OperationID             string
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
	LeaseToken              string
	LeaseExpiresAt          sql.NullTime
	HeartbeatAt             sql.NullTime
	CancellationRequestedAt sql.NullTime
	LastErrorClass          string
	LastErrorCode           string
	CreatedAt               time.Time
	StartedAt               sql.NullTime
	CompletedAt             sql.NullTime
}

// Failure is a bounded, safe failure classification. Detail and report data
// must not be stored in queue error fields.
type Failure struct {
	Class ErrorClass
	Code  string
}

// Metrics is a low-cardinality operational snapshot for one queue.
type Metrics struct {
	Queued             int64
	Running            int64
	RetryWait          int64
	DeadLetter         int64
	Succeeded          int64
	Cancelled          int64
	OldestQueuedAge    time.Duration
	AverageRunDuration time.Duration
	ExpiredLeases      int64
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
	if err := validateSpec(spec); err != nil {
		return Job{}, false, err
	}
	if spec.AvailableAt.IsZero() {
		spec.AvailableAt = time.Now().UTC()
	}
	id, err := randomID("job_", 20)
	if err != nil {
		return Job{}, false, err
	}

	result, err := q.db.ExecContext(ctx, `
		INSERT INTO open_aspm.jobs (
			id, workspace_id, operation_id, queue, kind, schema_version,
			payload, idempotency_key, priority, available_at, max_attempts
		) VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (workspace_id, queue, kind, idempotency_key) DO NOTHING`,
		id, spec.WorkspaceID, spec.OperationID, spec.Queue, spec.Kind,
		spec.SchemaVersion, []byte(spec.Payload), spec.IdempotencyKey,
		spec.Priority, spec.AvailableAt, spec.MaxAttempts,
	)
	if err != nil {
		return Job{}, false, fmt.Errorf("enqueue job: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Job{}, false, fmt.Errorf("read enqueue result: %w", err)
	}
	job, err := q.byIdempotencyKey(ctx, spec)
	if err != nil {
		return Job{}, false, err
	}
	created := rows == 1
	if !created && !sameSpec(job, spec) {
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
) (Job, bool, error) {
	if !validName(queue) || strings.TrimSpace(owner) == "" || len(owner) > 255 ||
		leaseDuration < minLease || leaseDuration > maxLease {
		return Job{}, false, ErrInvalid
	}
	token, err := randomID("lease_", 32)
	if err != nil {
		return Job{}, false, err
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin lease: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		WITH candidate AS (
			SELECT job.id
			FROM open_aspm.jobs AS job
			WHERE job.queue = $1
			  AND job.state IN ('queued', 'retry_wait')
			  AND job.available_at <= now()
			  AND job.cancellation_requested_at IS NULL
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
			lease_token = $3,
			lease_expires_at = now() + ($4 * interval '1 microsecond'),
			heartbeat_at = now()
		FROM candidate
		WHERE job.id = candidate.id
		RETURNING `+jobColumns("job"), queue, owner, token, leaseDuration.Microseconds())
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("acquire job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO open_aspm.job_attempts (
			job_id, attempt_number, lease_owner, last_heartbeat_at
		) VALUES ($1, $2, $3, now())`,
		job.ID, job.AttemptCount, owner,
	); err != nil {
		return Job{}, false, fmt.Errorf("record job attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, fmt.Errorf("commit lease: %w", err)
	}
	return job, true, nil
}

// Start marks a leased job as running. The lease token fences stale workers.
func (q *Queue) Start(ctx context.Context, id, token string) error {
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job start: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.jobs
		SET state = 'running', started_at = COALESCE(started_at, now())
		WHERE id = $1 AND lease_token = $2 AND state = 'leased'
		  AND lease_expires_at > now() AND cancellation_requested_at IS NULL`, id, token)
	if err != nil {
		return fmt.Errorf("start job: %w", err)
	}
	if err := requireLease(result); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return classifyLeaseMiss(ctx, tx, id, token)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts AS attempt
		SET started_at = COALESCE(started_at, now())
		FROM open_aspm.jobs AS job
		WHERE job.id = $1 AND job.lease_token = $2
		  AND attempt.job_id = job.id AND attempt.attempt_number = job.attempt_count`, id, token); err != nil {
		return fmt.Errorf("record job start: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job start: %w", err)
	}
	return nil
}

// Heartbeat extends a current lease.
func (q *Queue) Heartbeat(ctx context.Context, id, token string, leaseDuration time.Duration) error {
	if leaseDuration < minLease || leaseDuration > maxLease {
		return ErrInvalid
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin heartbeat: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.jobs
		SET heartbeat_at = now(), lease_expires_at = now() + ($3 * interval '1 microsecond')
		WHERE id = $1 AND lease_token = $2 AND state IN ('leased', 'running')
		  AND lease_expires_at > now() AND cancellation_requested_at IS NULL`,
		id, token, leaseDuration.Microseconds())
	if err != nil {
		return fmt.Errorf("heartbeat job: %w", err)
	}
	if err := requireLease(result); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return classifyLeaseMiss(ctx, tx, id, token)
		}
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts AS attempt
		SET last_heartbeat_at = now()
		FROM open_aspm.jobs AS job
		WHERE job.id = $1 AND job.lease_token = $2
		  AND attempt.job_id = job.id AND attempt.attempt_number = job.attempt_count`, id, token); err != nil {
		return fmt.Errorf("record heartbeat: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit heartbeat: %w", err)
	}
	return nil
}

// Succeed completes a running job.
func (q *Queue) Succeed(ctx context.Context, id, token string) error {
	return q.finish(ctx, id, token, StateSucceeded)
}

// Fail records a safe failure. Retryable failures wait until availableAt;
// permanent or exhausted failures move to dead-letter state.
func (q *Queue) Fail(
	ctx context.Context,
	id string,
	token string,
	failure Failure,
	availableAt time.Time,
) (State, error) {
	if (failure.Class != ErrorRetryable && failure.Class != ErrorPermanent) || !validCode(failure.Code) {
		return "", ErrInvalid
	}
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
		WHERE id = $1 AND lease_token = $2 AND state = 'running'
		  AND lease_expires_at > now()
		FOR UPDATE`, id, token).Scan(&attemptCount, &maxAttempts, &cancellationRequested)
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
		SET state = $3, available_at = CASE WHEN $3 = 'retry_wait' THEN $4 ELSE available_at END,
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			last_error_class = CASE WHEN $3 = 'cancelled' THEN last_error_class ELSE $5 END,
			last_error_code = CASE WHEN $3 = 'cancelled' THEN last_error_code ELSE $6 END,
			completed_at = CASE WHEN $3 IN ('dead_letter', 'cancelled') THEN now() ELSE NULL END
		WHERE id = $1 AND lease_token = $2`,
		id, token, state, availableAt, failure.Class, failure.Code); err != nil {
		return "", fmt.Errorf("fail job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts
		SET finished_at = now(), outcome = $3,
			safe_error_code = CASE WHEN $3 = 'cancelled' THEN NULL ELSE $4 END
		WHERE job_id = $1 AND attempt_number = $2`,
		id, attemptCount, outcome, failure.Code); err != nil {
		return "", fmt.Errorf("finish failed attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit job failure: %w", err)
	}
	return state, nil
}

// RequestCancellation durably records cancellation intent. Jobs that are not
// active are cancelled immediately.
func (q *Queue) RequestCancellation(ctx context.Context, id string) (State, error) {
	var state State
	err := q.db.QueryRowContext(ctx, `
		UPDATE open_aspm.jobs
		SET cancellation_requested_at = COALESCE(cancellation_requested_at, now()),
			state = CASE WHEN state IN ('queued', 'retry_wait') THEN 'cancelled' ELSE state END,
			completed_at = CASE WHEN state IN ('queued', 'retry_wait') THEN now() ELSE completed_at END
		WHERE id = $1 AND state NOT IN ('succeeded', 'dead_letter', 'cancelled')
		RETURNING state`, id).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if scanErr := q.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM open_aspm.jobs WHERE id = $1)`, id).Scan(&exists); scanErr != nil {
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
func (q *Queue) CancelActive(ctx context.Context, id, token string) error {
	return q.finish(ctx, id, token, StateCancelled)
}

// RecoverExpired reclaims expired leases in a bounded batch. Running jobs wait
// for retryDelay before they can be acquired again.
func (q *Queue) RecoverExpired(ctx context.Context, limit int, retryDelay time.Duration) (int, error) {
	if limit <= 0 || limit > 1000 || retryDelay < 0 {
		return 0, ErrInvalid
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin lease recovery: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, state, attempt_count, max_attempts,
		       cancellation_requested_at IS NOT NULL
		FROM open_aspm.jobs
		WHERE state IN ('leased', 'running') AND lease_expires_at <= now()
		ORDER BY lease_expires_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, limit)
	if err != nil {
		return 0, fmt.Errorf("select expired leases: %w", err)
	}
	type expired struct {
		id        string
		state     State
		attempt   int
		max       int
		cancelled bool
	}
	var jobs []expired
	for rows.Next() {
		var job expired
		if err := rows.Scan(&job.id, &job.state, &job.attempt, &job.max, &job.cancelled); err != nil {
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
		availableAt := time.Now().UTC()
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
			SET state = $2, available_at = $3,
				lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
				last_error_class = 'retryable', last_error_code = 'lease_expired',
				completed_at = CASE WHEN $4 THEN now() ELSE NULL END
			WHERE id = $1`, job.id, state, availableAt, completed); err != nil {
			return 0, fmt.Errorf("recover expired lease: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE open_aspm.job_attempts
			SET finished_at = now(), outcome = $3, safe_error_code = 'lease_expired'
			WHERE job_id = $1 AND attempt_number = $2`, job.id, job.attempt, outcome); err != nil {
			return 0, fmt.Errorf("finish expired attempt: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit lease recovery: %w", err)
	}
	return len(jobs), nil
}

// Metrics returns bounded aggregate data suitable for telemetry instruments.
func (q *Queue) Metrics(ctx context.Context, queue string) (Metrics, error) {
	if !validName(queue) {
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
			COALESCE(avg(EXTRACT(EPOCH FROM completed_at - started_at)) FILTER (
				WHERE completed_at IS NOT NULL AND started_at IS NOT NULL
			), 0),
			count(*) FILTER (WHERE state IN ('leased', 'running') AND lease_expires_at <= now())
		FROM open_aspm.jobs
		WHERE queue = $1`, queue).Scan(
		&metrics.Queued, &metrics.Running, &metrics.RetryWait,
		&metrics.DeadLetter, &metrics.Succeeded, &metrics.Cancelled,
		&oldestSeconds, &averageSeconds, &metrics.ExpiredLeases,
	)
	if err != nil {
		return Metrics{}, fmt.Errorf("read queue metrics: %w", err)
	}
	metrics.OldestQueuedAge = time.Duration(oldestSeconds * float64(time.Second))
	metrics.AverageRunDuration = time.Duration(averageSeconds * float64(time.Second))
	return metrics, nil
}

func (q *Queue) finish(ctx context.Context, id, token string, state State) error {
	if state != StateSucceeded && state != StateCancelled {
		return ErrInvalid
	}
	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin job completion: %w", err)
	}
	defer tx.Rollback()
	var attempt int
	err = tx.QueryRowContext(ctx, `
		UPDATE open_aspm.jobs
		SET state = $3, lease_owner = NULL, lease_token = NULL,
			lease_expires_at = NULL, completed_at = now()
		WHERE id = $1 AND lease_token = $2
		  AND (state = 'running' OR ($3 = 'cancelled' AND state = 'leased'))
		  AND lease_expires_at > now()
		  AND ($3 = 'cancelled' OR cancellation_requested_at IS NULL)
		RETURNING attempt_count`, id, token, state).Scan(&attempt)
	if errors.Is(err, sql.ErrNoRows) {
		if state == StateSucceeded {
			return classifyLeaseMiss(ctx, tx, id, token)
		}
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE open_aspm.job_attempts
		SET finished_at = now(), outcome = $3
		WHERE job_id = $1 AND attempt_number = $2`, id, attempt, state); err != nil {
		return fmt.Errorf("finish job attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit job completion: %w", err)
	}
	return nil
}

func (q *Queue) byIdempotencyKey(ctx context.Context, spec Spec) (Job, error) {
	row := q.db.QueryRowContext(ctx, `
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

func scanJob(row scanner) (Job, error) {
	var job Job
	var operationID, leaseOwner, leaseToken sql.NullString
	var lastErrorClass, lastErrorCode sql.NullString
	err := row.Scan(
		&job.ID, &job.WorkspaceID, &operationID, &job.Queue, &job.Kind,
		&job.SchemaVersion, &job.Payload, &job.IdempotencyKey, &job.State,
		&job.Priority, &job.AvailableAt, &job.AttemptCount, &job.MaxAttempts,
		&leaseOwner, &leaseToken, &job.LeaseExpiresAt, &job.HeartbeatAt,
		&job.CancellationRequestedAt, &lastErrorClass, &lastErrorCode,
		&job.CreatedAt, &job.StartedAt, &job.CompletedAt,
	)
	if err != nil {
		return Job{}, err
	}
	job.OperationID = operationID.String
	job.LeaseOwner = leaseOwner.String
	job.LeaseToken = leaseToken.String
	job.LastErrorClass = lastErrorClass.String
	job.LastErrorCode = lastErrorCode.String
	return job, nil
}

func jobColumns(alias string) string {
	return strings.Join([]string{
		alias + ".id", alias + ".workspace_id", alias + ".operation_id",
		alias + ".queue", alias + ".kind", alias + ".schema_version",
		alias + ".payload", alias + ".idempotency_key", alias + ".state",
		alias + ".priority", alias + ".available_at", alias + ".attempt_count",
		alias + ".max_attempts", alias + ".lease_owner", alias + ".lease_token",
		alias + ".lease_expires_at", alias + ".heartbeat_at",
		alias + ".cancellation_requested_at", alias + ".last_error_class",
		alias + ".last_error_code", alias + ".created_at", alias + ".started_at",
		alias + ".completed_at",
	}, ", ")
}

func validateSpec(spec Spec) error {
	if strings.TrimSpace(spec.WorkspaceID) == "" || len(spec.WorkspaceID) > 255 ||
		len(spec.OperationID) > 255 || !validName(spec.Queue) ||
		!validName(spec.Kind) || spec.SchemaVersion <= 0 ||
		strings.TrimSpace(spec.IdempotencyKey) == "" || len(spec.IdempotencyKey) > 255 ||
		spec.MaxAttempts <= 0 || spec.MaxAttempts > 100 ||
		len(spec.Payload) == 0 || len(spec.Payload) > maxPayloadBytes || !json.Valid(spec.Payload) {
		return ErrInvalid
	}
	return nil
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

func classifyLeaseMiss(ctx context.Context, tx *sql.Tx, id, token string) error {
	var cancellationRequested bool
	err := tx.QueryRowContext(ctx, `
		SELECT cancellation_requested_at IS NOT NULL
		FROM open_aspm.jobs
		WHERE id = $1 AND lease_token = $2 AND state IN ('leased', 'running')
		  AND lease_expires_at > now()`, id, token).Scan(&cancellationRequested)
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
