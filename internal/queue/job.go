package queue

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"time"
)

const DefaultQueue = "default"

var nextID atomic.Uint64

// Job is the unit of work processed by a queue worker.
type Job interface {
	Handle(context.Context) error
}

// JobFunc adapts a function into a Job.
type JobFunc func(context.Context) error

func (f JobFunc) Handle(ctx context.Context) error {
	if f == nil {
		return nil
	}

	return f(ctx)
}

// EnqueuedJob is the public metadata returned after a job has been queued.
type EnqueuedJob struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Queue       string    `json:"queue"`
	Attempts    int       `json:"attempts"`
	MaxAttempts int       `json:"max_attempts"`
	AvailableAt time.Time `json:"available_at"`
	CreatedAt   time.Time `json:"created_at"`
}

// Envelope is the queue payload plus worker metadata.
type Envelope struct {
	ID          string
	Name        string
	Queue       string
	Attempts    int
	MaxAttempts int
	Backoff     []time.Duration
	Timeout     time.Duration
	AvailableAt time.Time
	CreatedAt   time.Time
	Job         Job
}

func newEnvelope(job Job, options dispatchOptions) *Envelope {
	now := time.Now()
	queue := strings.TrimSpace(options.queue)
	if queue == "" {
		queue = DefaultQueue
	}

	maxAttempts := options.maxAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	delay := options.delay
	if delay < 0 {
		delay = 0
	}

	return &Envelope{
		ID:          newID(),
		Name:        jobName(job),
		Queue:       queue,
		MaxAttempts: maxAttempts,
		Backoff:     append([]time.Duration(nil), options.backoff...),
		Timeout:     options.timeout,
		AvailableAt: now.Add(delay),
		CreatedAt:   now,
		Job:         job,
	}
}

func (e *Envelope) Snapshot() EnqueuedJob {
	if e == nil {
		return EnqueuedJob{}
	}

	return EnqueuedJob{
		ID:          e.ID,
		Name:        e.Name,
		Queue:       e.Queue,
		Attempts:    e.Attempts,
		MaxAttempts: e.MaxAttempts,
		AvailableAt: e.AvailableAt,
		CreatedAt:   e.CreatedAt,
	}
}

func (e *Envelope) nextBackoff() time.Duration {
	if e == nil || len(e.Backoff) == 0 {
		return 0
	}

	index := e.Attempts - 1
	if index < 0 {
		index = 0
	}
	if index >= len(e.Backoff) {
		index = len(e.Backoff) - 1
	}

	delay := e.Backoff[index]
	if delay < 0 {
		return 0
	}

	return delay
}

func newID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), nextID.Add(1))
}

func jobName(job Job) string {
	if job == nil {
		return ""
	}

	type namedJob interface {
		Name() string
	}
	if named, ok := job.(namedJob); ok {
		name := strings.TrimSpace(named.Name())
		if name != "" {
			return name
		}
	}

	t := reflect.TypeOf(job)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	name := t.Name()
	if name == "" {
		return t.String()
	}
	if t.PkgPath() == "" {
		return name
	}

	return t.PkgPath() + "." + name
}
