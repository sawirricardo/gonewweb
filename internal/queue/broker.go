package queue

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

var ErrNoQueues = errors.New("queue: no queues configured")

// Broker stores queued jobs and reserves them for workers.
type Broker interface {
	Enqueue(context.Context, *Envelope) error
	Reserve(context.Context, []string) (*Envelope, error)
	Delete(context.Context, *Envelope) error
	Release(context.Context, *Envelope, time.Duration) error
	Fail(context.Context, *Envelope, error) error
}

type FailedJob struct {
	Job      EnqueuedJob
	Error    string
	FailedAt time.Time
}

// MemoryBroker is an in-process broker suitable for development and single
// process deployments.
type MemoryBroker struct {
	mu     sync.Mutex
	buffer int
	queues map[string]chan *Envelope
	failed []FailedJob
}

func NewMemoryBroker() *MemoryBroker {
	return NewMemoryBrokerWithBuffer(1024)
}

func NewMemoryBrokerWithBuffer(buffer int) *MemoryBroker {
	if buffer < 1 {
		buffer = 1
	}

	return &MemoryBroker{
		buffer: buffer,
		queues: make(map[string]chan *Envelope),
	}
}

func (b *MemoryBroker) Enqueue(ctx context.Context, envelope *Envelope) error {
	if b == nil {
		return ErrMissingBroker
	}
	if envelope == nil || envelope.Job == nil {
		return ErrMissingJob
	}
	if ctx == nil {
		ctx = context.Background()
	}

	envelope.Queue = normalizeQueue(envelope.Queue)
	ch := b.queue(envelope.Queue)
	delay := time.Until(envelope.AvailableAt)
	if delay <= 0 {
		select {
		case ch <- envelope:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	time.AfterFunc(delay, func() {
		ch <- envelope
	})

	return nil
}

func (b *MemoryBroker) Reserve(ctx context.Context, queues []string) (*Envelope, error) {
	if b == nil {
		return nil, ErrMissingBroker
	}
	if ctx == nil {
		ctx = context.Background()
	}

	queues = normalizeQueues(queues)
	if len(queues) == 0 {
		return nil, ErrNoQueues
	}

	channels := make([]chan *Envelope, 0, len(queues))
	for _, queue := range queues {
		channels = append(channels, b.queue(queue))
	}

	for {
		for _, ch := range channels {
			select {
			case envelope := <-ch:
				return envelope, nil
			default:
			}
		}

		cases := make([]reflect.SelectCase, 0, len(channels)+1)
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})
		for _, ch := range channels {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(ch),
			})
		}

		chosen, value, ok := reflect.Select(cases)
		if chosen == 0 {
			return nil, ctx.Err()
		}
		if !ok {
			continue
		}

		envelope, ok := value.Interface().(*Envelope)
		if !ok || envelope == nil {
			continue
		}

		return envelope, nil
	}
}

func (b *MemoryBroker) Delete(context.Context, *Envelope) error {
	if b == nil {
		return ErrMissingBroker
	}

	return nil
}

func (b *MemoryBroker) Release(ctx context.Context, envelope *Envelope, delay time.Duration) error {
	if b == nil {
		return ErrMissingBroker
	}
	if envelope == nil || envelope.Job == nil {
		return ErrMissingJob
	}
	if delay < 0 {
		delay = 0
	}

	envelope.AvailableAt = time.Now().Add(delay)
	return b.Enqueue(ctx, envelope)
}

func (b *MemoryBroker) Fail(_ context.Context, envelope *Envelope, cause error) error {
	if b == nil {
		return ErrMissingBroker
	}
	if envelope == nil {
		return ErrMissingJob
	}

	failed := FailedJob{
		Job:      envelope.Snapshot(),
		FailedAt: time.Now(),
	}
	if cause != nil {
		failed.Error = cause.Error()
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.failed = append(b.failed, failed)

	return nil
}

func (b *MemoryBroker) Failed() []FailedJob {
	if b == nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]FailedJob(nil), b.failed...)
}

func (b *MemoryBroker) queue(name string) chan *Envelope {
	name = normalizeQueue(name)

	b.mu.Lock()
	defer b.mu.Unlock()

	ch, ok := b.queues[name]
	if !ok {
		ch = make(chan *Envelope, b.buffer)
		b.queues[name] = ch
	}

	return ch
}

func normalizeQueue(queue string) string {
	queue = strings.TrimSpace(queue)
	if queue == "" {
		return DefaultQueue
	}

	return queue
}

func normalizeQueues(queues []string) []string {
	if len(queues) == 0 {
		return []string{DefaultQueue}
	}

	normalized := make([]string, 0, len(queues))
	seen := make(map[string]struct{}, len(queues))
	for _, queue := range queues {
		queue = normalizeQueue(queue)
		if _, ok := seen[queue]; ok {
			continue
		}
		seen[queue] = struct{}{}
		normalized = append(normalized, queue)
	}

	return normalized
}

func formatJobError(envelope *Envelope, err error) error {
	if envelope == nil {
		return err
	}
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s[%s]: %w", envelope.Name, envelope.ID, err)
}
