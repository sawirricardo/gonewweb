package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"gonewweb/internal/queue"
)

func main() {
	logger := newLogger()

	broker := queue.NewMemoryBroker()
	dispatcher := queue.NewDispatcher(broker)
	queueCtx, stopQueue := context.WithCancel(context.Background())
	defer stopQueue()

	workerCount := envInt("QUEUE_WORKERS", 1)
	if workerCount > 0 {
		worker := queue.NewWorker(broker, queue.WorkerConfig{
			Queues:      envCSV("QUEUE_QUEUES", []string{queue.DefaultQueue}),
			Concurrency: workerCount,
			Logger:      logger,
		})
		go func() {
			if err := worker.Run(queueCtx); err != nil {
				logger.Error("queue worker stopped", "error", err)
			}
		}()
	} else {
		logger.Info("queue worker disabled")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}

		fmt.Fprintln(w, "Hello World")
	})
	mux.HandleFunc("/queue-demo", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		message := r.URL.Query().Get("message")
		if message == "" {
			message = "hello from queue"
		}

		queueName := r.URL.Query().Get("queue")
		if queueName == "" {
			queueName = queue.DefaultQueue
		}

		delay, err := parseDurationParam(r.URL.Query().Get("delay"))
		if err != nil {
			http.Error(w, "invalid delay", http.StatusBadRequest)
			return
		}

		enqueued, err := dispatcher.Job(queue.JobFunc(func(ctx context.Context) error {
			logger.InfoContext(ctx, "demo queue job handled", "message", message, "queue", queueName)
			return nil
		})).
			OnQueue(queueName).
			Delay(delay).
			Attempts(3).
			Backoff(time.Second, 5*time.Second).
			Dispatch(r.Context())
		if err != nil {
			logger.ErrorContext(r.Context(), "queue dispatch failed", "error", err)
			http.Error(w, "queue dispatch failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(enqueued); err != nil {
			logger.ErrorContext(r.Context(), "queue response failed", "error", err)
		}
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	addr := ":" + port
	logger.Info("server listening", "addr", "http://localhost"+addr)
	if err := http.ListenAndServe(addr, requestLogger(logger, mux)); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func newLogger() *slog.Logger {
	if os.Getenv("LOG_FORMAT") == "json" {
		return slog.New(slog.NewJSONHandler(os.Stdout, nil))
	}

	return slog.New(slog.NewTextHandler(os.Stdout, nil))
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}

	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(data []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}

	n, err := r.ResponseWriter.Write(data)
	r.bytes += n
	return n, err
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func requestLogger(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &responseRecorder{ResponseWriter: w}

		next.ServeHTTP(recorder, r)

		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}

		logger.InfoContext(
			r.Context(),
			"request completed",
			"method", r.Method,
			"path", r.URL.Path,
			"status", status,
			"bytes", recorder.bytes,
			"duration", time.Since(start).String(),
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	})
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func envCSV(name string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parts := strings.Split(value, ",")
	items := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			items = append(items, part)
		}
	}
	if len(items) == 0 {
		return fallback
	}

	return items
}

func parseDurationParam(value string) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}

	duration, err := time.ParseDuration(value)
	if err == nil {
		if duration < 0 {
			return 0, nil
		}

		return duration, nil
	}

	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if seconds < 0 {
		return 0, nil
	}

	return time.Duration(seconds) * time.Second, nil
}
