//go:build integration

package jobqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spectremi/open-aspm/internal/database"
	"github.com/spectremi/open-aspm/internal/jobqueue"
)

func TestQueueLifecycleAndIdempotency(t *testing.T) {
	queue, db := openQueue(t)
	ctx := context.Background()
	spec := jobSpec("workspace-a", "same-key")

	created, wasCreated, err := queue.Enqueue(ctx, spec)
	if err != nil || !wasCreated {
		t.Fatalf("Enqueue() = (%+v, %t, %v), want created job", created, wasCreated, err)
	}
	replayed, wasCreated, err := queue.Enqueue(ctx, spec)
	if err != nil || wasCreated || replayed.ID != created.ID {
		t.Fatalf("replayed Enqueue() = (%+v, %t, %v)", replayed, wasCreated, err)
	}

	conflict := spec
	conflict.Payload = json.RawMessage(`{"import_id":"different"}`)
	if _, _, err := queue.Enqueue(ctx, conflict); !errors.Is(err, jobqueue.ErrConflict) {
		t.Fatalf("conflicting Enqueue() error = %v, want ErrConflict", err)
	}

	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 10)
	if err != nil || !ok || leased.ID != created.ID || leased.AttemptCount != 1 {
		t.Fatalf("Acquire() = (%+v, %t, %v)", leased, ok, err)
	}
	var storedTokenHash []byte
	if err := db.QueryRowContext(ctx, `
		SELECT lease_token_hash FROM open_aspm.jobs WHERE workspace_id = $1 AND id = $2`,
		leased.WorkspaceID, leased.ID,
	).Scan(&storedTokenHash); err != nil {
		t.Fatal(err)
	}
	if len(storedTokenHash) != 32 || string(storedTokenHash) == leased.FencingToken() {
		t.Fatal("lease token was not stored as a fixed-size one-way hash")
	}
	if err := queue.Start(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := queue.Heartbeat(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken(), time.Minute); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	retryAt := time.Now().Add(-time.Second)
	state, err := queue.Fail(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken(), jobqueue.Failure{
		Class: jobqueue.ErrorRetryable,
		Code:  "storage_unavailable",
	}, retryAt)
	if err != nil || state != jobqueue.StateRetryWait {
		t.Fatalf("Fail(retryable) = (%q, %v)", state, err)
	}

	retried, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute, 10)
	if err != nil || !ok || retried.ID != created.ID || retried.AttemptCount != 2 {
		t.Fatalf("retry Acquire() = (%+v, %t, %v)", retried, ok, err)
	}
	if err := queue.Start(ctx, retried.WorkspaceID, retried.ID, retried.FencingToken()); err != nil {
		t.Fatalf("retry Start() error = %v", err)
	}
	if err := queue.Succeed(ctx, retried.WorkspaceID, retried.ID, retried.FencingToken()); err != nil {
		t.Fatalf("Succeed() error = %v", err)
	}
	if err := queue.Succeed(ctx, retried.WorkspaceID, retried.ID, retried.FencingToken()); !errors.Is(err, jobqueue.ErrLeaseLost) {
		t.Fatalf("stale Succeed() error = %v, want ErrLeaseLost", err)
	}

	metrics, err := queue.Metrics(ctx, "ingestion", 10)
	if err != nil {
		t.Fatalf("Metrics() error = %v", err)
	}
	if metrics.Succeeded != 1 || metrics.RetryWait != 0 || metrics.DeadLetter != 0 || metrics.Retries != 1 {
		t.Fatalf("Metrics() = %+v", metrics)
	}
}

func TestConcurrentAcquisitionIsExclusive(t *testing.T) {
	queue, _ := openQueue(t)
	ctx := context.Background()
	const jobCount = 12
	for index := 0; index < jobCount; index++ {
		spec := jobSpec("workspace-"+strconv.Itoa(index%3), "key-"+strconv.Itoa(index))
		if _, _, err := queue.Enqueue(ctx, spec); err != nil {
			t.Fatalf("Enqueue(%d) error = %v", index, err)
		}
	}

	var wait sync.WaitGroup
	ids := make(chan string, jobCount)
	errorsFound := make(chan error, jobCount)
	for index := 0; index < jobCount; index++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			job, ok, err := queue.Acquire(ctx, "ingestion", fmt.Sprintf("worker-%d", worker), time.Minute, 10)
			if err != nil {
				errorsFound <- err
				return
			}
			if !ok {
				errorsFound <- errors.New("no job acquired")
				return
			}
			ids <- job.ID
		}(index)
	}
	wait.Wait()
	close(ids)
	close(errorsFound)
	for err := range errorsFound {
		t.Errorf("concurrent Acquire() error = %v", err)
	}
	seen := make(map[string]struct{}, jobCount)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Errorf("job %s leased more than once", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != jobCount {
		t.Fatalf("acquired %d unique jobs, want %d", len(seen), jobCount)
	}
}

func TestConcurrentEnqueueIsIdempotent(t *testing.T) {
	queue, db := openQueue(t)
	ctx := context.Background()
	const requestCount = 16
	results := make(chan struct {
		id      string
		created bool
		err     error
	}, requestCount)
	var wait sync.WaitGroup
	for index := 0; index < requestCount; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			job, created, err := queue.Enqueue(ctx, jobSpec("workspace-a", "concurrent-key"))
			results <- struct {
				id      string
				created bool
				err     error
			}{id: job.ID, created: created, err: err}
		}()
	}
	wait.Wait()
	close(results)

	var jobID string
	createdCount := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent Enqueue() error = %v", result.err)
		}
		if jobID == "" {
			jobID = result.id
		} else if result.id != jobID {
			t.Fatalf("concurrent Enqueue() returned job %q, want %q", result.id, jobID)
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent Enqueue() created %d jobs, want one", createdCount)
	}
	var storedCount int
	if err := db.QueryRowContext(ctx, `
		SELECT count(*) FROM open_aspm.jobs
		WHERE workspace_id = $1 AND queue = $2 AND kind = $3 AND idempotency_key = $4`,
		"workspace-a", "ingestion", "parse-report", "concurrent-key",
	).Scan(&storedCount); err != nil {
		t.Fatal(err)
	}
	if storedCount != 1 {
		t.Fatalf("stored job count = %d, want one", storedCount)
	}
}

func TestWorkspaceScopeAndInFlightCap(t *testing.T) {
	queue, _ := openQueue(t)
	ctx := context.Background()
	first, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cap-first"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cap-second"))
	if err != nil {
		t.Fatal(err)
	}
	lease, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 1)
	if err != nil || !ok || (lease.ID != first.ID && lease.ID != second.ID) {
		t.Fatalf("first Acquire() = (%+v, %t, %v)", lease, ok, err)
	}
	if _, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute, 1); err != nil || ok {
		t.Fatalf("Acquire() above workspace cap = (ok %t, %v), want no lease", ok, err)
	}
	metrics, err := queue.Metrics(ctx, "ingestion", 1)
	if err != nil || metrics.SaturatedWorkspaces != 1 {
		t.Fatalf("Metrics() = (%+v, %v), want one saturated workspace", metrics, err)
	}
	if err := queue.Start(ctx, "workspace-b", lease.ID, lease.FencingToken()); !errors.Is(err, jobqueue.ErrLeaseLost) {
		t.Fatalf("cross-workspace Start() error = %v, want ErrLeaseLost", err)
	}
	if _, err := queue.RequestCancellation(ctx, "workspace-b", lease.ID); !errors.Is(err, jobqueue.ErrNotFound) {
		t.Fatalf("cross-workspace RequestCancellation() error = %v, want ErrNotFound", err)
	}
	if err := queue.Start(ctx, lease.WorkspaceID, lease.ID, lease.FencingToken()); err != nil {
		t.Fatal(err)
	}
	if err := queue.Succeed(ctx, lease.WorkspaceID, lease.ID, lease.FencingToken()); err != nil {
		t.Fatal(err)
	}
	remaining, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute, 1)
	if err != nil || !ok || remaining.ID == lease.ID {
		t.Fatalf("Acquire() after releasing cap = (%+v, %t, %v)", remaining, ok, err)
	}
}

func TestQueueTransactionBoundaries(t *testing.T) {
	queue, db := openQueue(t)
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	rolledBack, _, err := queue.EnqueueTx(ctx, tx, jobSpec("workspace-a", "tx-rollback"))
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM open_aspm.jobs WHERE workspace_id = $1 AND id = $2)`,
		"workspace-a", rolledBack.ID,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("rolled-back EnqueueTx() left a visible job")
	}

	job, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "tx-success"))
	if err != nil {
		t.Fatal(err)
	}
	lease, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 1)
	if err != nil || !ok || lease.ID != job.ID {
		t.Fatalf("Acquire() = (%+v, %t, %v)", lease, ok, err)
	}
	if err := queue.Start(ctx, lease.WorkspaceID, lease.ID, lease.FencingToken()); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.SucceedTx(ctx, tx, lease.WorkspaceID, lease.ID, lease.FencingToken()); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := db.QueryRowContext(ctx, `
		SELECT state FROM open_aspm.jobs WHERE workspace_id = $1 AND id = $2`,
		lease.WorkspaceID, lease.ID,
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(jobqueue.StateRunning) {
		t.Fatalf("state after rolled-back SucceedTx() = %q, want running", state)
	}
}

func TestAcquisitionPrefersWorkspaceWithFewerActiveJobs(t *testing.T) {
	queue, _ := openQueue(t)
	ctx := context.Background()

	for index := 0; index < 2; index++ {
		spec := jobSpec("workspace-noisy", "noisy-"+strconv.Itoa(index))
		spec.Priority = 10
		if _, _, err := queue.Enqueue(ctx, spec); err != nil {
			t.Fatalf("Enqueue(noisy %d) error = %v", index, err)
		}
	}
	quiet, _, err := queue.Enqueue(ctx, jobSpec("workspace-quiet", "quiet"))
	if err != nil {
		t.Fatalf("Enqueue(quiet) error = %v", err)
	}

	first, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 10)
	if err != nil || !ok || first.WorkspaceID != "workspace-noisy" {
		t.Fatalf("first Acquire() = (%+v, %t, %v), want high-priority noisy workspace", first, ok, err)
	}
	second, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute, 10)
	if err != nil || !ok || second.ID != quiet.ID {
		t.Fatalf("second Acquire() = (%+v, %t, %v), want quiet workspace job %s", second, ok, err, quiet.ID)
	}
}

func TestCancellationAndExpiredLeaseRecovery(t *testing.T) {
	queue, db := openQueue(t)
	ctx := context.Background()

	queued, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cancel-queued"))
	if err != nil {
		t.Fatal(err)
	}
	state, err := queue.RequestCancellation(ctx, queued.WorkspaceID, queued.ID)
	if err != nil || state != jobqueue.StateCancelled {
		t.Fatalf("RequestCancellation(queued) = (%q, %v)", state, err)
	}

	leasedOnly, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cancel-leased"))
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 10)
	if err != nil || !ok || leased.ID != leasedOnly.ID {
		t.Fatalf("Acquire(leased cancellation) = (%+v, %t, %v)", leased, ok, err)
	}
	state, err = queue.RequestCancellation(ctx, leased.WorkspaceID, leased.ID)
	if err != nil || state != jobqueue.StateLeased {
		t.Fatalf("RequestCancellation(leased) = (%q, %v)", state, err)
	}
	if err := queue.Start(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); !errors.Is(err, jobqueue.ErrCancellationRequested) {
		t.Fatalf("Start(cancelled) error = %v, want ErrCancellationRequested", err)
	}
	if err := queue.CancelActive(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); err != nil {
		t.Fatalf("CancelActive(leased) error = %v", err)
	}

	active, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cancel-active"))
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err = queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 10)
	if err != nil || !ok || leased.ID != active.ID {
		t.Fatalf("Acquire(active) = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); err != nil {
		t.Fatal(err)
	}
	state, err = queue.RequestCancellation(ctx, leased.WorkspaceID, leased.ID)
	if err != nil || state != jobqueue.StateRunning {
		t.Fatalf("RequestCancellation(active) = (%q, %v)", state, err)
	}
	if err := queue.Heartbeat(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken(), time.Minute); !errors.Is(err, jobqueue.ErrCancellationRequested) {
		t.Fatalf("Heartbeat(cancelled) error = %v, want ErrCancellationRequested", err)
	}
	state, err = queue.Fail(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken(), jobqueue.Failure{
		Class: jobqueue.ErrorRetryable,
		Code:  "storage_unavailable",
	}, time.Now().Add(time.Minute))
	if err != nil || state != jobqueue.StateCancelled {
		t.Fatalf("Fail(cancelled) = (%q, %v)", state, err)
	}

	expiring, _, err := queue.Enqueue(ctx, jobSpec("workspace-b", "expires"))
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err = queue.Acquire(ctx, "ingestion", "worker-b", time.Minute, 10)
	if err != nil || !ok || leased.ID != expiring.ID {
		t.Fatalf("Acquire(expiring) = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE open_aspm.jobs SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	recovered, err := queue.RecoverExpired(ctx, "ingestion", 10, 0)
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = (%d, %v), want one", recovered, err)
	}
	if err := queue.Succeed(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); !errors.Is(err, jobqueue.ErrLeaseLost) {
		t.Fatalf("expired Succeed() error = %v, want ErrLeaseLost", err)
	}
	retried, ok, err := queue.Acquire(ctx, "ingestion", "worker-c", time.Minute, 10)
	if err != nil || !ok || retried.ID != expiring.ID || retried.AttemptCount != 2 {
		t.Fatalf("Acquire(recovered) = (%+v, %t, %v)", retried, ok, err)
	}
}

func TestPermanentFailureAndAttemptLimitDeadLetter(t *testing.T) {
	queue, _ := openQueue(t)
	ctx := context.Background()

	spec := jobSpec("workspace-a", "permanent")
	spec.MaxAttempts = 1
	created, _, err := queue.Enqueue(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute, 10)
	if err != nil || !ok || leased.ID != created.ID {
		t.Fatalf("Acquire() = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken()); err != nil {
		t.Fatal(err)
	}
	state, err := queue.Fail(ctx, leased.WorkspaceID, leased.ID, leased.FencingToken(), jobqueue.Failure{
		Class: jobqueue.ErrorRetryable,
		Code:  "temporary_failure",
	}, time.Now())
	if err != nil || state != jobqueue.StateDeadLetter {
		t.Fatalf("Fail(exhausted) = (%q, %v)", state, err)
	}
}

func jobSpec(workspace, key string) jobqueue.Spec {
	return jobqueue.Spec{
		WorkspaceID:           workspace,
		InitiatingPrincipalID: "svc_test",
		SystemCapability:      "imports:process",
		Queue:                 "ingestion",
		Kind:                  "parse-report",
		SchemaVersion:         1,
		Payload:               json.RawMessage(`{"import_id":"imp_example"}`),
		IdempotencyKey:        key,
		MaxAttempts:           3,
	}
}

func openQueue(t *testing.T) (*jobqueue.Queue, *sql.DB) {
	t.Helper()
	adminURL := os.Getenv("OPEN_ASPM_TEST_DATABASE_ADMIN_URL")
	if adminURL == "" {
		t.Skip("OPEN_ASPM_TEST_DATABASE_ADMIN_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	admin, err := sql.Open("pgx", adminURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("connect to test PostgreSQL: %v", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	databaseName := "open_aspm_queue_" + suffix
	roleName := "open_aspm_queue_owner_" + suffix
	runtimeRoleName := "open_aspm_queue_runtime_" + suffix
	password := "integration-test-only"
	roleIdentifier := pgx.Identifier{roleName}.Sanitize()
	runtimeRoleIdentifier := pgx.Identifier{runtimeRoleName}.Sanitize()
	databaseIdentifier := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
		roleIdentifier,
	)); err != nil {
		t.Fatalf("create test role: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
		runtimeRoleIdentifier,
	)); err != nil {
		t.Fatalf("create runtime test role: %v", err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE DATABASE %s OWNER %s", databaseIdentifier, roleIdentifier,
	)); err != nil {
		t.Fatalf("create test database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf(
			"DROP DATABASE IF EXISTS %s WITH (FORCE)", databaseIdentifier,
		))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf(
			"DROP ROLE IF EXISTS %s", runtimeRoleIdentifier,
		))
		_, _ = admin.ExecContext(cleanupCtx, fmt.Sprintf(
			"DROP ROLE IF EXISTS %s", roleIdentifier,
		))
	})

	databaseURL := testDatabaseURL(t, adminURL, databaseName, roleName, password)
	migrator, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open migrator: %v", err)
	}
	if _, _, err := migrator.Up(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
	if err := migrator.Close(); err != nil {
		t.Fatalf("close migrator: %v", err)
	}

	ownerDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerDB.Close()
	for _, workspaceID := range []string{
		"workspace-a", "workspace-b", "workspace-noisy", "workspace-quiet",
		"workspace-0", "workspace-1", "workspace-2",
	} {
		if _, err := ownerDB.ExecContext(ctx, `
			INSERT INTO open_aspm.workspaces (id) VALUES ($1)`, workspaceID); err != nil {
			t.Fatalf("create test workspace %s: %v", workspaceID, err)
		}
	}
	grants := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA open_aspm TO %s", runtimeRoleIdentifier),
		fmt.Sprintf("GRANT SELECT ON open_aspm.workspaces TO %s", runtimeRoleIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON open_aspm.jobs TO %s", runtimeRoleIdentifier),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE ON open_aspm.job_attempts TO %s", runtimeRoleIdentifier),
	}
	for _, grant := range grants {
		if _, err := ownerDB.ExecContext(ctx, grant); err != nil {
			t.Fatalf("grant queue runtime access: %v", err)
		}
	}

	runtimeURL := testDatabaseURL(t, adminURL, databaseName, runtimeRoleName, password)
	db, err := sql.Open("pgx", runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(20)
	queue, err := jobqueue.New(db)
	if err != nil {
		t.Fatal(err)
	}
	return queue, db
}

func testDatabaseURL(t *testing.T, baseURL, databaseName, user, password string) string {
	t.Helper()
	config, err := url.Parse(baseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	config.Path = "/" + databaseName
	config.User = url.UserPassword(user, password)
	return config.String()
}
