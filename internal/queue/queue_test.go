package queue

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatchAndWorkerProcessesJob(t *testing.T) {
	broker := NewMemoryBroker()
	dispatcher := NewDispatcher(broker)
	worker := NewWorker(broker, WorkerConfig{
		Logger: discardLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = worker.Run(ctx)
	}()

	var handled atomic.Int32
	_, err := dispatcher.Dispatch(ctx, JobFunc(func(context.Context) error {
		handled.Add(1)
		return nil
	}))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitUntil(t, func() bool {
		return handled.Load() == 1
	})
}

func TestPendingDispatchDelayAndQueue(t *testing.T) {
	broker := NewMemoryBroker()
	dispatcher := NewDispatcher(broker)
	worker := NewWorker(broker, WorkerConfig{
		Queues: []string{"emails"},
		Logger: discardLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = worker.Run(ctx)
	}()

	handled := make(chan struct{}, 1)
	_, err := dispatcher.Job(JobFunc(func(context.Context) error {
		handled <- struct{}{}
		return nil
	})).
		OnQueue("emails").
		Delay(35 * time.Millisecond).
		Dispatch(ctx)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	select {
	case <-handled:
		t.Fatal("job ran before delay elapsed")
	case <-time.After(15 * time.Millisecond):
	}

	select {
	case <-handled:
	case <-time.After(time.Second):
		t.Fatal("job did not run")
	}
}

func TestWorkerRetriesWithBackoff(t *testing.T) {
	broker := NewMemoryBroker()
	dispatcher := NewDispatcher(broker)
	worker := NewWorker(broker, WorkerConfig{
		Logger: discardLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = worker.Run(ctx)
	}()

	var attempts atomic.Int32
	_, err := dispatcher.Job(JobFunc(func(context.Context) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary failure")
		}
		return nil
	})).
		Attempts(2).
		Backoff(10 * time.Millisecond).
		Dispatch(ctx)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitUntil(t, func() bool {
		return attempts.Load() == 2
	})

	if failed := broker.Failed(); len(failed) != 0 {
		t.Fatalf("expected no failed jobs, got %d", len(failed))
	}
}

func TestWorkerRecordsFailedJob(t *testing.T) {
	broker := NewMemoryBroker()
	dispatcher := NewDispatcher(broker)
	worker := NewWorker(broker, WorkerConfig{
		Logger: discardLogger(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = worker.Run(ctx)
	}()

	var attempts atomic.Int32
	_, err := dispatcher.Job(JobFunc(func(context.Context) error {
		attempts.Add(1)
		return errors.New("permanent failure")
	})).
		Attempts(2).
		Backoff(time.Millisecond).
		Dispatch(ctx)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	waitUntil(t, func() bool {
		return len(broker.Failed()) == 1
	})

	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
	failed := broker.Failed()[0]
	if failed.Job.Attempts != 2 {
		t.Fatalf("expected failed attempts to be 2, got %d", failed.Job.Attempts)
	}
	if failed.Error == "" {
		t.Fatal("expected failed job error")
	}
}

func TestDispatchSync(t *testing.T) {
	dispatcher := NewDispatcher(nil)

	var handled atomic.Int32
	err := dispatcher.Job(JobFunc(func(context.Context) error {
		handled.Add(1)
		return nil
	})).DispatchSync(context.Background())
	if err != nil {
		t.Fatalf("dispatch sync: %v", err)
	}
	if handled.Load() != 1 {
		t.Fatalf("expected job to run once, got %d", handled.Load())
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}
