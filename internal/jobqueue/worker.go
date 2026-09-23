package jobqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"
)

// OutcomeKind is the bounded result returned by a job handler.
type OutcomeKind string

const (
	OutcomeSucceeded OutcomeKind = "succeeded"
	OutcomeRetryable OutcomeKind = "retryable"
	OutcomePermanent OutcomeKind = "permanent"
	OutcomeCancelled OutcomeKind = "cancelled"
)

// Outcome contains only a stable code safe for persistence and telemetry.
type Outcome struct {
	Kind       OutcomeKind
	Code       string
	RetryAfter time.Duration
}

// Handler performs one idempotent unit of work. Before loading sensitive data
// or applying domain writes, handlers must authorize the job's recorded
// workspace, initiating principal, and system capability through the owning
// application service. A valid lease is not authorization.
type Handler func(context.Context, Job) Outcome

type queueBackend interface {
	Acquire(context.Context, string, string, time.Duration, int) (Lease, bool, error)
	Start(context.Context, string, string, string) error
	Heartbeat(context.Context, string, string, string, time.Duration) error
	Succeed(context.Context, string, string, string) error
	Fail(context.Context, string, string, string, Failure, time.Time) (State, error)
	CancelActive(context.Context, string, string, string) error
}

// WorkerConfig controls bounded polling, leases, and retry behavior.
type WorkerConfig struct {
	Queue                   string
	Owner                   string
	Concurrency             int
	MaxInFlightPerWorkspace int
	LeaseDuration           time.Duration
	HeartbeatInterval       time.Duration
	PollInterval            time.Duration
	RetryMinimum            time.Duration
	RetryMaximum            time.Duration
	RetryMaximumAge         time.Duration
}

// Worker leases jobs and dispatches them to handlers by job kind.
type Worker struct {
	queue    queueBackend
	config   WorkerConfig
	handlers map[string]Handler
	now      func() time.Time
	jitter   func(int, time.Duration, time.Duration) time.Duration
}

// NewWorker validates a worker configuration and copies the handler registry.
func NewWorker(queue *Queue, config WorkerConfig, handlers map[string]Handler) (*Worker, error) {
	if queue == nil || !validName(config.Queue) || strings.TrimSpace(config.Owner) != config.Owner ||
		config.Owner == "" || strings.IndexByte(config.Owner, 0) >= 0 || len(config.Owner) > 120 ||
		config.Concurrency <= 0 || config.Concurrency > 128 ||
		config.MaxInFlightPerWorkspace <= 0 || config.MaxInFlightPerWorkspace > 100 ||
		config.LeaseDuration < minLease || config.LeaseDuration > maxLease ||
		config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.LeaseDuration ||
		config.PollInterval <= 0 || config.RetryMinimum <= 0 ||
		config.RetryMaximum < config.RetryMinimum || config.RetryMaximumAge < config.RetryMaximum {
		return nil, ErrInvalid
	}
	copied := make(map[string]Handler, len(handlers))
	for kind, handler := range handlers {
		if !validName(kind) || handler == nil {
			return nil, ErrInvalid
		}
		copied[kind] = handler
	}
	return &Worker{
		queue: queue, config: config, handlers: copied,
		now: time.Now, jitter: jitteredRetryDelay,
	}, nil
}

// Run processes jobs until the context is cancelled or queue access fails.
func (worker *Worker) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsFound := make(chan error, worker.config.Concurrency)
	var wait sync.WaitGroup
	for index := 0; index < worker.config.Concurrency; index++ {
		wait.Add(1)
		go func(slot int) {
			defer wait.Done()
			owner := fmt.Sprintf("%s-%d", worker.config.Owner, slot)
			if err := worker.runSlot(ctx, owner); err != nil && !errors.Is(err, context.Canceled) {
				select {
				case errorsFound <- err:
				default:
				}
				cancel()
			}
		}(index)
	}
	wait.Wait()
	select {
	case err := <-errorsFound:
		return err
	default:
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return ctx.Err()
	}
}

func (worker *Worker) runSlot(ctx context.Context, owner string) error {
	for {
		lease, ok, err := worker.queue.Acquire(
			ctx,
			worker.config.Queue,
			owner,
			worker.config.LeaseDuration,
			worker.config.MaxInFlightPerWorkspace,
		)
		if err != nil {
			return err
		}
		if !ok {
			if err := waitFor(ctx, worker.config.PollInterval); err != nil {
				return err
			}
			continue
		}
		if err := worker.process(ctx, lease); err != nil {
			if errors.Is(err, ErrLeaseLost) {
				continue
			}
			return err
		}
	}
}

func (worker *Worker) process(ctx context.Context, lease Lease) error {
	job := lease.Job
	token := lease.FencingToken()
	if err := worker.queue.Start(ctx, job.WorkspaceID, job.ID, token); err != nil {
		if errors.Is(err, ErrCancellationRequested) {
			return worker.queue.CancelActive(ctx, job.WorkspaceID, job.ID, token)
		}
		return err
	}
	handler := worker.handlers[job.Kind]
	if handler == nil {
		_, err := worker.queue.Fail(ctx, job.WorkspaceID, job.ID, token, Failure{
			Class: ErrorPermanent,
			Code:  "unsupported_job_kind",
		}, time.Time{})
		return err
	}

	handlerContext, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	heartbeatStopped := make(chan struct{})
	heartbeatErrors := make(chan error, 1)
	go func() {
		defer close(heartbeatStopped)
		ticker := time.NewTicker(worker.config.HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-handlerContext.Done():
				return
			case <-ticker.C:
				if err := worker.queue.Heartbeat(
					handlerContext,
					job.WorkspaceID,
					job.ID,
					token,
					worker.config.LeaseDuration,
				); err != nil {
					select {
					case <-handlerContext.Done():
						return
					default:
					}
					heartbeatErrors <- err
					cancelHandler()
					return
				}
			}
		}
	}()

	outcome := callHandler(handlerContext, handler, job)
	cancelHandler()
	<-heartbeatStopped
	select {
	case err := <-heartbeatErrors:
		if errors.Is(err, ErrCancellationRequested) {
			return worker.queue.CancelActive(ctx, job.WorkspaceID, job.ID, token)
		}
		return err
	default:
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	switch outcome.Kind {
	case OutcomeSucceeded:
		err := worker.queue.Succeed(ctx, job.WorkspaceID, job.ID, token)
		if errors.Is(err, ErrCancellationRequested) {
			return worker.queue.CancelActive(ctx, job.WorkspaceID, job.ID, token)
		}
		return err
	case OutcomeCancelled:
		return worker.queue.CancelActive(ctx, job.WorkspaceID, job.ID, token)
	case OutcomeRetryable:
		if !validCode(outcome.Code) {
			return worker.failInvalidOutcome(ctx, job, token)
		}
		jitter := worker.jitter
		if jitter == nil {
			jitter = jitteredRetryDelay
		}
		now := time.Now
		if worker.now != nil {
			now = worker.now
		}
		nowValue := now().UTC()
		remainingAge := worker.config.RetryMaximumAge - nowValue.Sub(job.CreatedAt)
		if remainingAge <= 0 {
			_, err := worker.queue.Fail(ctx, job.WorkspaceID, job.ID, token, Failure{
				Class: ErrorPermanent,
				Code:  outcome.Code,
			}, time.Time{})
			return err
		}
		delay := jitter(job.AttemptCount, worker.config.RetryMinimum, worker.config.RetryMaximum)
		if outcome.RetryAfter > delay {
			delay = outcome.RetryAfter
		}
		if delay > worker.config.RetryMaximum {
			delay = worker.config.RetryMaximum
		}
		if delay > remainingAge {
			delay = remainingAge
		}
		_, err := worker.queue.Fail(ctx, job.WorkspaceID, job.ID, token, Failure{
			Class: ErrorRetryable,
			Code:  outcome.Code,
		}, nowValue.Add(delay))
		return err
	case OutcomePermanent:
		if !validCode(outcome.Code) {
			return worker.failInvalidOutcome(ctx, job, token)
		}
		_, err := worker.queue.Fail(ctx, job.WorkspaceID, job.ID, token, Failure{
			Class: ErrorPermanent,
			Code:  outcome.Code,
		}, time.Time{})
		return err
	default:
		return worker.failInvalidOutcome(ctx, job, token)
	}
}

func (worker *Worker) failInvalidOutcome(ctx context.Context, job Job, token string) error {
	_, err := worker.queue.Fail(ctx, job.WorkspaceID, job.ID, token, Failure{
		Class: ErrorPermanent,
		Code:  "invalid_handler_outcome",
	}, time.Time{})
	return err
}

func callHandler(ctx context.Context, handler Handler, job Job) (outcome Outcome) {
	defer func() {
		if recover() != nil {
			outcome = Outcome{Kind: OutcomeRetryable, Code: "handler_panic"}
		}
	}()
	return handler(ctx, job)
}

func retryDelay(attempt int, minimum, maximum time.Duration) time.Duration {
	if minimum <= 0 || maximum <= 0 {
		return 0
	}
	delay := minimum
	for current := 1; current < attempt && delay < maximum; current++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func jitteredRetryDelay(attempt int, minimum, maximum time.Duration) time.Duration {
	upper := retryDelay(attempt, minimum, maximum)
	if upper <= 1 {
		return upper
	}
	lower := upper / 2
	spread := upper - lower
	random, err := rand.Int(rand.Reader, big.NewInt(int64(spread)+1))
	if err != nil {
		return upper
	}
	return lower + time.Duration(random.Int64())
}

func waitFor(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
