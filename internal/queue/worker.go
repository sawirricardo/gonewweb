package queue

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

type WorkerConfig struct {
	Queues      []string
	Concurrency int
	Logger      *slog.Logger
}

type Worker struct {
	broker      Broker
	queues      []string
	concurrency int
	logger      *slog.Logger
}

func NewWorker(broker Broker, config WorkerConfig) *Worker {
	concurrency := config.Concurrency
	if concurrency < 1 {
		concurrency = 1
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Worker{
		broker:      broker,
		queues:      normalizeQueues(config.Queues),
		concurrency: concurrency,
		logger:      logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if w == nil || w.broker == nil {
		return ErrMissingBroker
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var wg sync.WaitGroup
	wg.Add(w.concurrency)
	for workerID := 1; workerID <= w.concurrency; workerID++ {
		go func(workerID int) {
			defer wg.Done()
			w.runLoop(ctx, workerID)
		}(workerID)
	}

	wg.Wait()
	return nil
}

func (w *Worker) runLoop(ctx context.Context, workerID int) {
	for {
		envelope, err := w.broker.Reserve(ctx, w.queues)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			w.logger.ErrorContext(ctx, "queue reserve failed", "error", err, "worker_id", workerID)
			time.Sleep(250 * time.Millisecond)
			continue
		}

		w.process(ctx, workerID, envelope)
	}
}

func (w *Worker) process(workerCtx context.Context, workerID int, envelope *Envelope) {
	if envelope == nil {
		return
	}

	envelope.Attempts++
	start := time.Now()
	jobCtx := context.WithoutCancel(workerCtx)

	w.logger.InfoContext(
		jobCtx,
		"queue job started",
		"id", envelope.ID,
		"name", envelope.Name,
		"queue", envelope.Queue,
		"attempt", envelope.Attempts,
		"max_attempts", envelope.MaxAttempts,
		"worker_id", workerID,
	)

	err := runJob(jobCtx, envelope)
	if err == nil {
		if deleteErr := w.broker.Delete(context.Background(), envelope); deleteErr != nil {
			w.logger.ErrorContext(jobCtx, "queue job delete failed", "id", envelope.ID, "error", deleteErr)
			return
		}

		w.logger.InfoContext(
			jobCtx,
			"queue job completed",
			"id", envelope.ID,
			"name", envelope.Name,
			"queue", envelope.Queue,
			"attempts", envelope.Attempts,
			"duration", time.Since(start).String(),
			"worker_id", workerID,
		)
		return
	}

	err = formatJobError(envelope, err)
	if envelope.Attempts < envelope.MaxAttempts {
		delay := envelope.nextBackoff()
		jobID := envelope.ID
		jobName := envelope.Name
		queueName := envelope.Queue
		attempt := envelope.Attempts
		maxAttempts := envelope.MaxAttempts
		if releaseErr := w.broker.Release(context.Background(), envelope, delay); releaseErr != nil {
			w.logger.ErrorContext(jobCtx, "queue job release failed", "id", jobID, "error", releaseErr)
			_ = w.broker.Fail(context.Background(), envelope, releaseErr)
			return
		}

		w.logger.WarnContext(
			jobCtx,
			"queue job released",
			"id", jobID,
			"name", jobName,
			"queue", queueName,
			"attempt", attempt,
			"max_attempts", maxAttempts,
			"backoff", delay.String(),
			"error", err,
			"worker_id", workerID,
		)
		return
	}

	if failErr := w.broker.Fail(context.Background(), envelope, err); failErr != nil {
		w.logger.ErrorContext(jobCtx, "queue job fail record failed", "id", envelope.ID, "error", failErr)
		return
	}

	w.logger.ErrorContext(
		jobCtx,
		"queue job failed",
		"id", envelope.ID,
		"name", envelope.Name,
		"queue", envelope.Queue,
		"attempts", envelope.Attempts,
		"duration", time.Since(start).String(),
		"error", err,
		"worker_id", workerID,
	)
}

func runJob(ctx context.Context, envelope *Envelope) (err error) {
	if envelope == nil || envelope.Job == nil {
		return ErrMissingJob
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if envelope.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, envelope.Timeout)
		defer cancel()
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("panic: %v\n%s", recovered, string(debug.Stack()))
		}
	}()

	return envelope.Job.Handle(ctx)
}
