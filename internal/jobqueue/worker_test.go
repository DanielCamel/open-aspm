package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type workerQueueStub struct {
	mu                sync.Mutex
	heartbeatErr      error
	succeedErr        error
	cancelActiveCalls int
}

func (*workerQueueStub) Acquire(context.Context, string, string, time.Duration) (Job, bool, error) {
	return Job{}, false, nil
}

func (*workerQueueStub) Start(context.Context, string, string) error {
	return nil
}

func (queue *workerQueueStub) Heartbeat(context.Context, string, string, time.Duration) error {
	return queue.heartbeatErr
}

func (queue *workerQueueStub) Succeed(context.Context, string, string) error {
	return queue.succeedErr
}

func (*workerQueueStub) Fail(context.Context, string, string, Failure, time.Time) (State, error) {
	return "", errors.New("unexpected Fail call")
}

func (queue *workerQueueStub) CancelActive(context.Context, string, string) error {
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

	err := worker.process(context.Background(), Job{
		ID:         "job_example",
		Kind:       "parse-report",
		LeaseToken: "lease_example",
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

	err := worker.process(context.Background(), Job{
		ID:         "job_example",
		Kind:       "parse-report",
		LeaseToken: "lease_example",
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
