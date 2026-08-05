// SPDX-License-Identifier: Apache-2.0
// Package logging configures the process-wide structured logger (slog) and
// provides the two correlation primitives the admin gateway uses:
//
//   - request ids: chi's RequestID middleware + RequestLogger emit one JSON
//     line per HTTP request; FromRequest gives handlers a logger carrying the
//     same request_id so handler-level lines join up with the access line.
//   - operation ids: long-running operations (snapshot, rolling update, ...)
//     run in goroutines that outlive the triggering request. NewOpID mints an
//     id that is logged by the spawning handler (with the request_id) and by
//     every line the operation goroutine emits, tying the two together.
package logging

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5/middleware"
)

// Setup installs the process-wide default logger. format is "json" (default)
// or "text" (human-readable, for terminals).
func Setup(format string) {
	var h slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if format == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	} else {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	slog.SetDefault(slog.New(h))
}

// Component returns a logger tagged with the emitting subsystem
// (pm, ops, handler, autosnapshot, status, ...).
func Component(name string) *slog.Logger {
	return slog.Default().With("component", name)
}

// opSeq counts operations within this process; the process start time (fixed
// once at init) disambiguates ids across admin restarts, so op_id is unique
// in the journal without needing randomness.
var opSeq atomic.Int64

var procEpoch = time.Now().Unix()

// NewOpID mints a correlation id for a long-running operation goroutine.
func NewOpID(operation string) string {
	return fmt.Sprintf("%s-%d-%d", operation, procEpoch, opSeq.Add(1))
}

// FromRequest returns a logger carrying the request's correlation id.
func FromRequest(r *http.Request) *slog.Logger {
	l := Component("handler")
	if reqID := middleware.GetReqID(r.Context()); reqID != "" {
		l = l.With("request_id", reqID)
	}
	return l
}

// RequestLogger is the structured replacement for chi's middleware.Logger:
// one line per request with method, path, status, size, duration and the
// request_id minted by middleware.RequestID (which must run before this).
func RequestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// High-frequency scrape/poll endpoints would drown the journal.
		if r.URL.Path == "/health" || r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		slog.Default().LogAttrs(r.Context(), slog.LevelInfo, "http request",
			slog.String("component", "http"),
			slog.String("request_id", middleware.GetReqID(r.Context())),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("remote", r.RemoteAddr),
			slog.Int("status", ww.Status()),
			slog.Int("bytes", ww.BytesWritten()),
			slog.Duration("duration", time.Since(start)),
		)
	})
}
