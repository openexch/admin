// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/openexch/admin/services"
)

// Tunables for the SSE stream; vars so tests can shrink them.
var (
	eventsHeartbeat    = 15 * time.Second
	eventsProgressPoll = 250 * time.Millisecond
)

// handleEvents streams agent lifecycle events and operation progress as
// Server-Sent Events — the first consumer of the ProcessAgent Subscribe
// stream. Two named event types share the stream:
//
//	event: process  — one agent.Event (started/stopped/crashed/cascade-stop/
//	                  disarmed/adopted), JSON verbatim
//	event: cluster  — one services.ClusterEvent (leader change, node up/down,
//	                  quorum lost/restored) for ANY cluster, diffed out of the
//	                  status poll. Retained history is replayed on connect.
//	event: progress — the /api/admin/progress map, emitted on connect and
//	                  whenever it changes (poll-based; replaces the UI's
//	                  50ms HTTP fast-poll during operations)
//
// Process delivery is best-effort BY DESIGN: Subscribe's bounded buffer drops
// events for a slow consumer rather than wedging the crash path. Cluster events
// are the operator's record of what happened to the money path, so those get a
// server-side backlog replayed on connect instead of being live-only. Clients
// re-seed from /api/admin/status after (re)connecting; /status stays the source
// of truth for current state.
func (h *Handlers) handleEvents(w http.ResponseWriter, r *http.Request) {
	// ResponseController unwraps middleware ResponseWriter wrappers (chi's
	// request logger, the metrics statusWriter) to reach the real Flusher —
	// a bare http.Flusher type assert fails behind them.
	rc := http.NewResponseController(w)

	events, unsub := h.procMgr.Subscribe(64)
	defer unsub()

	var clusterEvents <-chan services.ClusterEvent
	if h.clusterEvents != nil {
		ch, unsubCluster := h.clusterEvents.Subscribe(64)
		defer unsubCluster()
		clusterEvents = ch
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if err := rc.Flush(); err != nil {
		// No flush support anywhere in the chain: nothing to stream over.
		return
	}

	writeEvent := func(name string, v interface{}) {
		data, err := json.Marshal(v)
		if err != nil {
			return
		}
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, data)
		rc.Flush()
	}

	// Replay the cluster backlog before anything live: an operator opening the
	// console after an incident needs the elections that already happened, not
	// only the ones that happen while they watch. This is what the trading UI
	// was using localStorage for, done server-side so it is shared and survives
	// a browser that forgot.
	if h.clusterEvents != nil {
		for _, ev := range h.clusterEvents.Recent(0) {
			writeEvent("cluster", ev)
		}
	}

	// Initial progress snapshot so a client connecting mid-operation renders
	// the bar immediately.
	lastSig := progressSignature(h.progress.ToMap())
	writeEvent("progress", h.progress.ToMap())

	hb := time.NewTicker(eventsHeartbeat)
	defer hb.Stop()
	prog := time.NewTicker(eventsProgressPoll)
	defer prog.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-events:
			if !ok {
				return
			}
			writeEvent("process", ev)
		case ev, ok := <-clusterEvents:
			// A nil channel blocks forever, so an unregistered log simply never
			// selects — no branch guard needed.
			if !ok {
				return
			}
			writeEvent("cluster", ev)
		case <-prog.C:
			m := h.progress.ToMap()
			if sig := progressSignature(m); !bytes.Equal(sig, lastSig) {
				lastSig = sig
				writeEvent("progress", m)
			}
		case <-hb.C:
			// Comment frame: keeps proxies and EventSource from timing out.
			fmt.Fprint(w, ": hb\n\n")
			rc.Flush()
		}
	}
}

// progressSignature is the change-detection key for progress frames:
// everything except elapsedMs, which advances every read and would otherwise
// re-emit an unchanged operation four times a second.
func progressSignature(m map[string]interface{}) []byte {
	sig := make(map[string]interface{}, len(m))
	for k, v := range m {
		if k == "elapsedMs" {
			continue
		}
		sig[k] = v
	}
	b, _ := json.Marshal(sig)
	return b
}
