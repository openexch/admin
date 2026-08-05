// SPDX-License-Identifier: Apache-2.0
package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/logging"
	"github.com/openexch/admin/services"
)

// eventsFakeAgent implements just enough of agent.ProcessAgent for the SSE
// handler: a scriptable Subscribe channel that records unsubscription.
type eventsFakeAgent struct {
	mu       sync.Mutex
	ch       chan agent.Event
	unsubbed bool
}

var _ agent.ProcessAgent = (*eventsFakeAgent)(nil)

func (f *eventsFakeAgent) Subscribe(buf int) (<-chan agent.Event, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ch = make(chan agent.Event, buf)
	return f.ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.unsubbed = true
	}
}

func (f *eventsFakeAgent) send(ev agent.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ch <- ev
}

func (f *eventsFakeAgent) wasUnsubscribed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unsubbed
}

func (f *eventsFakeAgent) List() []agent.ProcessInfo             { return nil }
func (f *eventsFakeAgent) Get(string) *agent.ProcessInfo         { return nil }
func (f *eventsFakeAgent) Summary() map[string]interface{}       { return nil }
func (f *eventsFakeAgent) Start(string) error                    { return nil }
func (f *eventsFakeAgent) StartUnchecked(string) error           { return nil }
func (f *eventsFakeAgent) Stop(string) error                     { return nil }
func (f *eventsFakeAgent) ForceStop(string) error                { return nil }
func (f *eventsFakeAgent) Restart(string) error                  { return nil }
func (f *eventsFakeAgent) StartAll() []agent.ActionResult        { return nil }
func (f *eventsFakeAgent) StopAll() []agent.ActionResult         { return nil }
func (f *eventsFakeAgent) RestartAll() []agent.ActionResult      { return nil }
func (f *eventsFakeAgent) TailLog(string, int) ([]string, error) { return nil, nil }
func (f *eventsFakeAgent) NodeCounters(int) (*agent.CounterData, error) {
	return nil, fmt.Errorf("not implemented")
}
func (f *eventsFakeAgent) InstallArtifact(agent.ArtifactSpec, io.Reader) error { return nil }
func (f *eventsFakeAgent) Close()                                              {}

// sseFrame is one parsed "event:/data:" pair.
type sseFrame struct {
	event string
	data  string
}

// readFrames consumes SSE frames from the response body until n frames were
// read or the timeout fires. Heartbeat comments are skipped.
func readFrames(t *testing.T, body io.Reader, n int, timeout time.Duration) []sseFrame {
	t.Helper()
	frames := make([]sseFrame, 0, n)
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(body)
		var cur sseFrame
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event: "):
				cur.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				cur.data = strings.TrimPrefix(line, "data: ")
			case line == "" && cur.event != "":
				frames = append(frames, cur)
				cur = sseFrame{}
				if len(frames) >= n {
					return
				}
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("timed out after %s waiting for %d frames (got %d)", timeout, n, len(frames))
	}
	return frames
}

// newEventsServer wires a real HTTP server (real Flusher, cancellable
// contexts) around the events handler.
func newEventsServer(t *testing.T, fake *eventsFakeAgent, progress *services.Progress) *httptest.Server {
	t.Helper()
	h := &Handlers{procMgr: fake, progress: progress}
	r := chi.NewRouter()
	r.Get("/api/admin/events", h.handleEvents)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func TestEventsStreamsProcessEvents(t *testing.T) {
	fake := &eventsFakeAgent{}
	srv := newEventsServer(t, fake, services.NewProgress())

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("wrong content type %q", ct)
	}

	fake.send(agent.Event{Type: agent.EventCrashed, Service: "node1", PID: 4242, Detail: "exit status 137"})

	// Frame 1 is the initial progress snapshot, frame 2 the crash.
	frames := readFrames(t, resp.Body, 2, 5*time.Second)
	if frames[0].event != "progress" {
		t.Fatalf("first frame should be the progress snapshot, got %q", frames[0].event)
	}
	if frames[1].event != "process" {
		t.Fatalf("expected a process frame, got %+v", frames[1])
	}
	var ev agent.Event
	if err := json.Unmarshal([]byte(frames[1].data), &ev); err != nil {
		t.Fatalf("process frame is not an agent.Event: %v (%q)", err, frames[1].data)
	}
	if ev.Type != agent.EventCrashed || ev.Service != "node1" || ev.PID != 4242 {
		t.Fatalf("event fields lost in transit: %+v", ev)
	}

	// Disconnect must unsubscribe (no leaked hub entries).
	cancel()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !fake.wasUnsubscribed() {
		time.Sleep(10 * time.Millisecond)
	}
	if !fake.wasUnsubscribed() {
		t.Fatal("client disconnect did not unsubscribe from the agent")
	}
}

func TestEventsStreamsProgressChangesOnly(t *testing.T) {
	oldPoll := eventsProgressPoll
	eventsProgressPoll = 10 * time.Millisecond
	defer func() { eventsProgressPoll = oldPoll }()

	fake := &eventsFakeAgent{}
	progress := services.NewProgress()
	srv := newEventsServer(t, fake, progress)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Initial snapshot arrives on connect.
	frames := readFrames(t, resp.Body, 1, 5*time.Second)
	if frames[0].event != "progress" {
		t.Fatalf("first frame should be progress, got %+v", frames[0])
	}

	if !progress.TryStart("housekeeping", 3) {
		t.Fatal("claim failed")
	}
	frames = readFrames(t, resp.Body, 1, 5*time.Second)

	progress.Update(1, "purging")
	frames = readFrames(t, resp.Body, 1, 5*time.Second)
	var last map[string]interface{}
	if err := json.Unmarshal([]byte(frames[0].data), &last); err != nil {
		t.Fatal(err)
	}
	if last["operation"] != "housekeeping" || last["status"] != "purging" {
		t.Fatalf("progress payload wrong: %v", last)
	}

	// An unchanged operation must NOT re-emit (elapsedMs alone changes every
	// read and is excluded from the signature). Expect silence: only the
	// heartbeat-free absence of frames within a few poll cycles.
	extra := make(chan sseFrame, 1)
	go func() {
		sc := bufio.NewScanner(resp.Body)
		var cur sseFrame
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "event: ") {
				cur.event = strings.TrimPrefix(line, "event: ")
			} else if line == "" && cur.event != "" {
				extra <- cur
				return
			}
		}
	}()
	select {
	case f := <-extra:
		t.Fatalf("unchanged progress re-emitted: %+v", f)
	case <-time.After(100 * time.Millisecond): // ~10 poll cycles of silence
	}
}

// TestEventsStreamsThroughMiddlewareStack reproduces the live wiring (chi
// request logger + metrics middleware, both of which wrap ResponseWriter):
// the stream must still reach a Flusher through the wrappers. Direct
// http.Flusher asserts fail here — this is the regression test for the
// "streaming unsupported" bug found on the live box.
func TestEventsStreamsThroughMiddlewareStack(t *testing.T) {
	fake := &eventsFakeAgent{}
	progress := services.NewProgress()
	h := &Handlers{procMgr: fake, progress: progress}

	metrics := services.NewMetricsService(nil, nil, fake, progress, nil)
	r := chi.NewRouter()
	r.Use(logging.RequestLogger)
	r.Use(metrics.Middleware)
	r.Get("/api/admin/events", h.handleEvents)
	srv := httptest.NewServer(r)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	fake.send(agent.Event{Type: agent.EventStarted, Service: "sim", PID: 1})
	frames := readFrames(t, resp.Body, 2, 5*time.Second)
	if frames[0].event != "progress" || frames[1].event != "process" {
		t.Fatalf("expected progress snapshot then process frame through the middleware stack, got %+v", frames)
	}
}

// TestEventsAuthIsHeaderOnly pins the auth posture for the stream: the
// standard bearer header works, and a token in the URL NEVER does — URL
// tokens leak into history, referrers and intermediary logs. Browser clients
// that need a token must use fetch()-streaming (which can set headers), not
// EventSource.
func TestEventsAuthIsHeaderOnly(t *testing.T) {
	const token = "sekrit"
	fake := &eventsFakeAgent{}
	h := &Handlers{procMgr: fake, progress: services.NewProgress()}
	r := chi.NewRouter()
	r.Use(AuthMiddleware(token))
	r.Get("/api/admin/events", h.handleEvents)
	srv := httptest.NewServer(r)
	defer srv.Close()

	get := func(path, bearer string) int {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+path, nil)
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if got := get("/api/admin/events", ""); got != http.StatusUnauthorized {
		t.Fatalf("no credentials should 401, got %d", got)
	}
	if got := get("/api/admin/events?token="+token, ""); got != http.StatusUnauthorized {
		t.Fatalf("a URL token must NOT authenticate, got %d", got)
	}
	if got := get("/api/admin/events", token); got != http.StatusOK {
		t.Fatalf("bearer header should 200, got %d", got)
	}
}

// newClusterEventsServer is newEventsServer with the cluster log registered.
func newClusterEventsServer(t *testing.T, fake *eventsFakeAgent, log *services.ClusterEventLog) *httptest.Server {
	t.Helper()
	h := &Handlers{procMgr: fake, progress: services.NewProgress()}
	h.SetClusterEvents(log)
	r := chi.NewRouter()
	r.Get("/api/admin/events", h.handleEvents)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

func clusterHealthy(n int) []string {
	h := make([]string, n)
	for i := range h {
		h[i] = services.HealthHealthy
	}
	return h
}

// An operator opening the console after an incident needs the elections that
// already happened, not only the ones that occur while they watch. The trading
// UI solved this with localStorage, which made the record per-browser; the
// backlog is replayed from the server instead.
func TestEventsReplaysClusterBacklogOnConnect(t *testing.T) {
	log := services.NewClusterEventLog()
	log.Observe("assets", "Assets Engine", 0, clusterHealthy(3))
	down := clusterHealthy(3)
	down[2] = services.HealthDead
	log.Observe("assets", "Assets Engine", 0, down)

	srv := newClusterEventsServer(t, &eventsFakeAgent{}, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// The backlog comes BEFORE the progress snapshot, so history renders in
	// order rather than after whatever is live.
	frames := readFrames(t, resp.Body, 2, 5*time.Second)
	if frames[0].event != "cluster" {
		t.Fatalf("first frame = %q, want the replayed cluster backlog", frames[0].event)
	}
	if !strings.Contains(frames[0].data, services.ClusterEventNodeDown) {
		t.Errorf("backlog frame = %s, want the NODE_DOWN that happened before connect", frames[0].data)
	}
	if !strings.Contains(frames[0].data, `"cluster":"assets"`) {
		t.Errorf("backlog frame = %s, want the assets cluster named", frames[0].data)
	}
	if frames[1].event != "progress" {
		t.Fatalf("second frame = %q, want the progress snapshot", frames[1].event)
	}
}

func TestEventsStreamsLiveClusterTransitions(t *testing.T) {
	log := services.NewClusterEventLog()
	log.Observe("match", "Matching Engine", 0, clusterHealthy(3))

	srv := newClusterEventsServer(t, &eventsFakeAgent{}, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Frame 1 is the progress snapshot (no backlog yet), then the election.
	first := readFrames(t, resp.Body, 1, 5*time.Second)
	if first[0].event != "progress" {
		t.Fatalf("first frame = %q, want progress", first[0].event)
	}

	log.Observe("match", "Matching Engine", 2, clusterHealthy(3))

	frames := readFrames(t, resp.Body, 1, 5*time.Second)
	if frames[0].event != "cluster" {
		t.Fatalf("frame = %q, want a live cluster frame", frames[0].event)
	}
	if !strings.Contains(frames[0].data, services.ClusterEventLeaderChange) {
		t.Errorf("frame = %s, want LEADER_CHANGE", frames[0].data)
	}
}

// A gateway with no cluster log registered must still stream process frames
// rather than selecting on a nil channel forever.
func TestEventsWithoutClusterLogStillStreams(t *testing.T) {
	fake := &eventsFakeAgent{}
	srv := newEventsServer(t, fake, services.NewProgress())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/admin/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	fake.send(agent.Event{Type: agent.EventStarted, Service: "oms"})

	frames := readFrames(t, resp.Body, 2, 5*time.Second)
	if frames[1].event != "process" {
		t.Fatalf("frame = %q, want a process frame", frames[1].event)
	}
}
