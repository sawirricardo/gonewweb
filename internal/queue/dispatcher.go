package queue

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	ErrMissingBroker = errors.New("queue: missing broker")
	ErrMissingJob    = errors.New("queue: missing job")
)

type dispatchOptions struct {
	queue       string
	delay       time.Duration
	maxAttempts int
	backoff     []time.Duration
	timeout     time.Duration
}

// DispatchOption configures a queued job before it is dispatched.
type DispatchOption func(*dispatchOptions)

// OnQueue routes the job to a named queue.
func OnQueue(name string) DispatchOption {
	return func(options *dispatchOptions) {
		options.queue = strings.TrimSpace(name)
	}
}

// Delay makes the job available after the given duration.
func Delay(delay time.Duration) DispatchOption {
	return func(options *dispatchOptions) {
		if delay < 0 {
			delay = 0
		}
		options.delay = delay
	}
}

// Attempts sets the maximum number of times a job may be attempted.
func Attempts(attempts int) DispatchOption {
	return func(options *dispatchOptions) {
		if attempts < 1 {
			attempts = 1
		}
		options.maxAttempts = attempts
	}
}

// Backoff configures retry delays. The last value is reused for extra attempts.
func Backoff(delays ...time.Duration) DispatchOption {
	return func(options *dispatchOptions) {
		options.backoff = options.backoff[:0]
		for _, delay := range delays {
			if delay < 0 {
				delay = 0
			}
			options.backoff = append(options.backoff, delay)
		}
	}
}

// Timeout sets a per-attempt job timeout.
func Timeout(timeout time.Duration) DispatchOption {
	return func(options *dispatchOptions) {
		if timeout < 0 {
			timeout = 0
		}
		options.timeout = timeout
	}
}

// Dispatcher queues jobs through a Broker.
type Dispatcher struct {
	broker   Broker
	defaults dispatchOptions
}

func NewDispatcher(broker Broker, options ...DispatchOption) *Dispatcher {
	defaults := defaultDispatchOptions()
	applyDispatchOptions(&defaults, options...)

	return &Dispatcher{
		broker:   broker,
		defaults: defaults,
	}
}

func (d *Dispatcher) Job(job Job) *PendingDispatch {
	options := defaultDispatchOptions()
	if d != nil {
		options = d.defaults.clone()
	}

	return &PendingDispatch{
		dispatcher: d,
		job:        job,
		options:    options,
	}
}

func (d *Dispatcher) Dispatch(ctx context.Context, job Job, options ...DispatchOption) (EnqueuedJob, error) {
	dispatchOptions := defaultDispatchOptions()
	if d != nil {
		dispatchOptions = d.defaults.clone()
	}
	applyDispatchOptions(&dispatchOptions, options...)

	return d.dispatch(ctx, job, dispatchOptions)
}

func (d *Dispatcher) Later(ctx context.Context, delay time.Duration, job Job, options ...DispatchOption) (EnqueuedJob, error) {
	options = append([]DispatchOption{Delay(delay)}, options...)
	return d.Dispatch(ctx, job, options...)
}

func (d *Dispatcher) DispatchSync(ctx context.Context, job Job, options ...DispatchOption) error {
	if job == nil {
		return ErrMissingJob
	}

	dispatchOptions := defaultDispatchOptions()
	if d != nil {
		dispatchOptions = d.defaults.clone()
	}
	applyDispatchOptions(&dispatchOptions, options...)

	return runJob(ctx, newEnvelope(job, dispatchOptions))
}

func (d *Dispatcher) dispatch(ctx context.Context, job Job, options dispatchOptions) (EnqueuedJob, error) {
	if d == nil || d.broker == nil {
		return EnqueuedJob{}, ErrMissingBroker
	}
	if job == nil {
		return EnqueuedJob{}, ErrMissingJob
	}
	if ctx == nil {
		ctx = context.Background()
	}

	envelope := newEnvelope(job, options)
	snapshot := envelope.Snapshot()
	if err := d.broker.Enqueue(ctx, envelope); err != nil {
		return EnqueuedJob{}, err
	}

	return snapshot, nil
}

type PendingDispatch struct {
	dispatcher *Dispatcher
	job        Job
	options    dispatchOptions
}

func (p *PendingDispatch) OnQueue(name string) *PendingDispatch {
	OnQueue(name)(&p.options)
	return p
}

func (p *PendingDispatch) Delay(delay time.Duration) *PendingDispatch {
	Delay(delay)(&p.options)
	return p
}

func (p *PendingDispatch) Attempts(attempts int) *PendingDispatch {
	Attempts(attempts)(&p.options)
	return p
}

func (p *PendingDispatch) Backoff(delays ...time.Duration) *PendingDispatch {
	Backoff(delays...)(&p.options)
	return p
}

func (p *PendingDispatch) Timeout(timeout time.Duration) *PendingDispatch {
	Timeout(timeout)(&p.options)
	return p
}

func (p *PendingDispatch) Dispatch(ctx context.Context) (EnqueuedJob, error) {
	if p == nil {
		return EnqueuedJob{}, ErrMissingJob
	}
	if p.dispatcher == nil {
		return EnqueuedJob{}, ErrMissingBroker
	}

	return p.dispatcher.dispatch(ctx, p.job, p.options)
}

func (p *PendingDispatch) DispatchSync(ctx context.Context) error {
	if p == nil {
		return ErrMissingJob
	}
	if p.dispatcher == nil {
		return runJob(ctx, newEnvelope(p.job, p.options))
	}

	return p.dispatcher.DispatchSync(ctx, p.job, optionsFromDispatchOptions(p.options)...)
}

func defaultDispatchOptions() dispatchOptions {
	return dispatchOptions{
		queue:       DefaultQueue,
		maxAttempts: 1,
	}
}

func (o dispatchOptions) clone() dispatchOptions {
	o.backoff = append([]time.Duration(nil), o.backoff...)
	return o
}

func applyDispatchOptions(target *dispatchOptions, options ...DispatchOption) {
	for _, option := range options {
		if option != nil {
			option(target)
		}
	}
	if strings.TrimSpace(target.queue) == "" {
		target.queue = DefaultQueue
	}
	if target.maxAttempts < 1 {
		target.maxAttempts = 1
	}
	if target.delay < 0 {
		target.delay = 0
	}
	if target.timeout < 0 {
		target.timeout = 0
	}
}

func optionsFromDispatchOptions(options dispatchOptions) []DispatchOption {
	return []DispatchOption{
		OnQueue(options.queue),
		Delay(options.delay),
		Attempts(options.maxAttempts),
		Backoff(options.backoff...),
		Timeout(options.timeout),
	}
}
