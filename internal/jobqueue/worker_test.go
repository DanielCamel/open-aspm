package jobqueue

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

type workerQueueStub struct {
	mu                sync.Mutex
	heartbeatErr      error
	succeedErr        error
	cancelActiveCalls int
	failCalls         int
	failure           Failure
	availableAt       time.Time
}

func (*workerQueueStub) Acquire(context.Context, string, string, time.Duration, int) (Lease, bool, error) {
	return Lease{}, false, nil
}

func (*workerQueueStub) Start(context.Context, string, string, string) error {
	return nil
}

func (queue *workerQueueStub) Heartbeat(context.Context, string, string, string, time.Duration) error {
	return queue.heartbeatErr
}

func (queue *workerQueueStub) Succeed(context.Context, string, string, string) error {
	return queue.succeedErr
}

func (queue *workerQueueStub) Fail(
	_ context.Context,
	_, _, _ string,
	failure Failure,
	availableAt time.Time,
) (State, error) {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.failCalls++
	queue.failure = failure
	queue.availableAt = availableAt
	return StateDeadLetter, nil
}

func (queue *workerQueueStub) CancelActive(context.Context, string, string, string) error {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	queue.cancelActiveCalls++
	return nil
}

func (queue *workerQueueStub) cancellationCount() int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return queue.cancelActiveCalls
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 5 * time.Second},
		{attempt: 20, want: 5 * time.Second},
	}
	for _, test := range tests {
		if got := retryDelay(test.attempt, time.Second, 5*time.Second); got != test.want {
			t.Errorf("retryDelay(%d) = %v, want %v", test.attempt, got, test.want)
		}
	}
}

func TestJitteredRetryDelayStaysWithinBounds(t *testing.T) {
	for index := 0; index < 100; index++ {
		delay := jitteredRetryDelay(3, time.Second, 10*time.Second)
		if delay < 2*time.Second || delay > 4*time.Second {
			t.Fatalf("jitteredRetryDelay() = %v, want [2s, 4s]", delay)
		}
	}
}

func TestCallHandlerConvertsPanicToRetry(t *testing.T) {
	outcome := callHandler(context.Background(), func(context.Context, Job) Outcome {
		panic("report content must not escape into an error")
	}, Job{})
	if outcome.Kind != OutcomeRetryable || outcome.Code != "handler_panic" {
		t.Fatalf("callHandler() = %+v", outcome)
	}
}

func TestSameSpecPreservesJSONNumberPrecision(t *testing.T) {
	job := Job{
		OperationID:   "operation-example",
		SchemaVersion: 1,
		Payload:       json.RawMessage(`{"finding":9007199254740992,"nested":{"b":2,"a":1}}`),
		MaxAttempts:   3,
	}
	equivalent := Spec{
		OperationID:   job.OperationID,
		SchemaVersion: job.SchemaVersion,
		Payload:       json.RawMessage(`{"nested":{"a":1,"b":2},"finding":9007199254740992}`),
		MaxAttempts:   job.MaxAttempts,
	}
	if !sameSpec(job, equivalent) {
		t.Fatal("sameSpec() rejected semantically equivalent JSON")
	}

	different := equivalent
	different.Payload = json.RawMessage(`{"nested":{"a":1,"b":2},"finding":9007199254740993}`)
	if sameSpec(job, different) {
		t.Fatal("sameSpec() treated distinct large JSON integers as equal")
	}
}

func TestWorkerAcknowledgesCancellationDetectedAtCompletion(t *testing.T) {
	queue := &workerQueueStub{succeedErr: ErrCancellationRequested}
	worker := &Worker{
		queue: queue,
		config: WorkerConfig{
			LeaseDuration:     time.Minute,
			HeartbeatInterval: time.Second,
		},
		handlers: map[string]Handler{
			"parse-report": func(context.Context, Job) Outcome {
				return Outcome{Kind: OutcomeSucceeded}
			},
		},
	}

	err := worker.process(context.Background(), Lease{
		Job:   Job{ID: "job_example", WorkspaceID: "wsp_example", Kind: "parse-report"},
		token: "lease_example",
	})
	if err != nil {
		t.Fatalf("process() error = %v", err)
	}
	if got := queue.cancellationCount(); got != 1 {
		t.Fatalf("CancelActive calls = %d, want 1", got)
	}
}

func TestWorkerAcknowledgesCancellationDetectedByHeartbeat(t *testing.T) {
	queue := &workerQueueStub{heartbeatErr: ErrCancellationRequested}
	worker := &Worker{
		queue: queue,
		config: WorkerConfig{
			LeaseDuration:     time.Minute,
			HeartbeatInterval: time.Millisecond,
		},
		handlers: map[string]Handler{
			"parse-report": func(ctx context.Context, _ Job) Outcome {
				<-ctx.Done()
				return Outcome{Kind: OutcomeSucceeded}
			},
		},
	}

	err := worker.process(context.Background(), Lease{
		Job:   Job{ID: "job_example", WorkspaceID: "wsp_example", Kind: "parse-report"},
		token: "lease_example",
	})
	if err != nil {
		t.Fatalf("process() error = %v", err)
	}
	if got := queue.cancellationCount(); got != 1 {
		t.Fatalf("CancelActive calls = %d, want 1", got)
	}
}

func TestWaitForCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitFor(ctx, time.Hour); err != context.Canceled {
		t.Fatalf("waitFor() error = %v, want context.Canceled", err)
	}
}

func TestWorkerStopsRetryAtMaximumAge(t *testing.T) {
	fixedNow := time.Date(2026, 9, 23, 12, 0, 0, 0, time.UTC)
	queue := &workerQueueStub{}
	worker := &Worker{
		queue: queue,
		config: WorkerConfig{
			LeaseDuration:     time.Minute,
			HeartbeatInterval: time.Second,
			RetryMinimum:      time.Second,
			RetryMaximum:      time.Minute,
			RetryMaximumAge:   time.Hour,
		},
		handlers: map[string]Handler{
			"parse-report": func(context.Context, Job) Outcome {
				return Outcome{Kind: OutcomeRetryable, Code: "storage_unavailable"}
			},
		},
		now:    func() time.Time { return fixedNow },
		jitter: func(int, time.Duration, time.Duration) time.Duration { return 30 * time.Second },
	}
	err := worker.process(context.Background(), Lease{
		Job: Job{
			ID: "job_example", WorkspaceID: "wsp_example", Kind: "parse-report",
			CreatedAt: fixedNow.Add(-2 * time.Hour), AttemptCount: 2,
		},
		token: "lease_example",
	})
	if err != nil {
		t.Fatal(err)
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.failCalls != 1 || queue.failure.Class != ErrorPermanent || !queue.availableAt.IsZero() {
		t.Fatalf("Fail() = calls %d, failure %+v, available %v", queue.failCalls, queue.failure, queue.availableAt)
	}
}
