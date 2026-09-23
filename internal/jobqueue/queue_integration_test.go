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
	queue, _ := openQueue(t)
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

	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute)
	if err != nil || !ok || leased.ID != created.ID || leased.AttemptCount != 1 {
		t.Fatalf("Acquire() = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	if err := queue.Heartbeat(ctx, leased.ID, leased.LeaseToken, time.Minute); err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}

	retryAt := time.Now().Add(-time.Second)
	state, err := queue.Fail(ctx, leased.ID, leased.LeaseToken, jobqueue.Failure{
		Class: jobqueue.ErrorRetryable,
		Code:  "storage_unavailable",
	}, retryAt)
	if err != nil || state != jobqueue.StateRetryWait {
		t.Fatalf("Fail(retryable) = (%q, %v)", state, err)
	}

	retried, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute)
	if err != nil || !ok || retried.ID != created.ID || retried.AttemptCount != 2 {
		t.Fatalf("retry Acquire() = (%+v, %t, %v)", retried, ok, err)
	}
	if err := queue.Start(ctx, retried.ID, retried.LeaseToken); err != nil {
		t.Fatalf("retry Start() error = %v", err)
	}
	if err := queue.Succeed(ctx, retried.ID, retried.LeaseToken); err != nil {
		t.Fatalf("Succeed() error = %v", err)
	}
	if err := queue.Succeed(ctx, retried.ID, retried.LeaseToken); !errors.Is(err, jobqueue.ErrLeaseLost) {
		t.Fatalf("stale Succeed() error = %v, want ErrLeaseLost", err)
	}

	metrics, err := queue.Metrics(ctx, "ingestion")
	if err != nil {
		t.Fatalf("Metrics() error = %v", err)
	}
	if metrics.Succeeded != 1 || metrics.RetryWait != 0 || metrics.DeadLetter != 0 {
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
			job, ok, err := queue.Acquire(ctx, "ingestion", fmt.Sprintf("worker-%d", worker), time.Minute)
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

	first, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute)
	if err != nil || !ok || first.WorkspaceID != "workspace-noisy" {
		t.Fatalf("first Acquire() = (%+v, %t, %v), want high-priority noisy workspace", first, ok, err)
	}
	second, ok, err := queue.Acquire(ctx, "ingestion", "worker-b", time.Minute)
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
	state, err := queue.RequestCancellation(ctx, queued.ID)
	if err != nil || state != jobqueue.StateCancelled {
		t.Fatalf("RequestCancellation(queued) = (%q, %v)", state, err)
	}

	leasedOnly, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cancel-leased"))
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute)
	if err != nil || !ok || leased.ID != leasedOnly.ID {
		t.Fatalf("Acquire(leased cancellation) = (%+v, %t, %v)", leased, ok, err)
	}
	state, err = queue.RequestCancellation(ctx, leased.ID)
	if err != nil || state != jobqueue.StateLeased {
		t.Fatalf("RequestCancellation(leased) = (%q, %v)", state, err)
	}
	if err := queue.Start(ctx, leased.ID, leased.LeaseToken); !errors.Is(err, jobqueue.ErrCancellationRequested) {
		t.Fatalf("Start(cancelled) error = %v, want ErrCancellationRequested", err)
	}
	if err := queue.CancelActive(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatalf("CancelActive(leased) error = %v", err)
	}

	active, _, err := queue.Enqueue(ctx, jobSpec("workspace-a", "cancel-active"))
	if err != nil {
		t.Fatal(err)
	}
	leased, ok, err = queue.Acquire(ctx, "ingestion", "worker-a", time.Minute)
	if err != nil || !ok || leased.ID != active.ID {
		t.Fatalf("Acquire(active) = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatal(err)
	}
	state, err = queue.RequestCancellation(ctx, leased.ID)
	if err != nil || state != jobqueue.StateRunning {
		t.Fatalf("RequestCancellation(active) = (%q, %v)", state, err)
	}
	if err := queue.Heartbeat(ctx, leased.ID, leased.LeaseToken, time.Minute); !errors.Is(err, jobqueue.ErrCancellationRequested) {
		t.Fatalf("Heartbeat(cancelled) error = %v, want ErrCancellationRequested", err)
	}
	state, err = queue.Fail(ctx, leased.ID, leased.LeaseToken, jobqueue.Failure{
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
	leased, ok, err = queue.Acquire(ctx, "ingestion", "worker-b", time.Minute)
	if err != nil || !ok || leased.ID != expiring.ID {
		t.Fatalf("Acquire(expiring) = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE open_aspm.jobs SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1`, leased.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	recovered, err := queue.RecoverExpired(ctx, 10, 0)
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverExpired() = (%d, %v), want one", recovered, err)
	}
	if err := queue.Succeed(ctx, leased.ID, leased.LeaseToken); !errors.Is(err, jobqueue.ErrLeaseLost) {
		t.Fatalf("expired Succeed() error = %v, want ErrLeaseLost", err)
	}
	retried, ok, err := queue.Acquire(ctx, "ingestion", "worker-c", time.Minute)
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
	leased, ok, err := queue.Acquire(ctx, "ingestion", "worker-a", time.Minute)
	if err != nil || !ok || leased.ID != created.ID {
		t.Fatalf("Acquire() = (%+v, %t, %v)", leased, ok, err)
	}
	if err := queue.Start(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatal(err)
	}
	state, err := queue.Fail(ctx, leased.ID, leased.LeaseToken, jobqueue.Failure{
		Class: jobqueue.ErrorRetryable,
		Code:  "temporary_failure",
	}, time.Now())
	if err != nil || state != jobqueue.StateDeadLetter {
		t.Fatalf("Fail(exhausted) = (%q, %v)", state, err)
	}
}

func jobSpec(workspace, key string) jobqueue.Spec {
	return jobqueue.Spec{
		WorkspaceID:    workspace,
		Queue:          "ingestion",
		Kind:           "parse-report",
		SchemaVersion:  1,
		Payload:        json.RawMessage(`{"import_id":"imp_example"}`),
		IdempotencyKey: key,
		MaxAttempts:    3,
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
	password := "integration-test-only"
	roleIdentifier := pgx.Identifier{roleName}.Sanitize()
	databaseIdentifier := pgx.Identifier{databaseName}.Sanitize()
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD 'integration-test-only' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION",
		roleIdentifier,
	)); err != nil {
		t.Fatalf("create test role: %v", err)
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

	db, err := sql.Open("pgx", databaseURL)
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
