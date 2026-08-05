// SPDX-License-Identifier: Apache-2.0
package services

import (
	"fmt"
	"io"
	"sync"

	"github.com/openexch/admin/agent"
)

// fakeAgent is a scriptable agent.ProcessAgent test double: services under
// test observe the scripted ProcessInfo catalog, and the fake records every
// lifecycle call and InstallArtifact spec for assertions.
type fakeAgent struct {
	mu    sync.Mutex
	procs map[string]*agent.ProcessInfo

	starts    []string
	stops     []string
	restarts  []string
	installed []agent.ArtifactSpec
	startErr  error
}

var _ agent.ProcessAgent = (*fakeAgent)(nil)

func newFakeAgent() *fakeAgent {
	return &fakeAgent{procs: make(map[string]*agent.ProcessInfo)}
}

func (f *fakeAgent) set(info agent.ProcessInfo) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.procs[info.Name] = &info
}

func (f *fakeAgent) List() []agent.ProcessInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]agent.ProcessInfo, 0, len(f.procs))
	for _, p := range f.procs {
		out = append(out, *p)
	}
	return out
}

func (f *fakeAgent) Get(name string) *agent.ProcessInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	if p, ok := f.procs[name]; ok {
		cp := *p
		return &cp
	}
	return nil
}

func (f *fakeAgent) Summary() map[string]interface{} {
	return map[string]interface{}{}
}

func (f *fakeAgent) Start(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts = append(f.starts, name)
	return f.startErr
}

func (f *fakeAgent) StartUnchecked(name string) error { return f.Start(name) }

func (f *fakeAgent) Stop(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops = append(f.stops, name)
	return nil
}

func (f *fakeAgent) ForceStop(name string) error { return f.Stop(name) }

func (f *fakeAgent) Restart(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restarts = append(f.restarts, name)
	return nil
}

func (f *fakeAgent) StartAll() []agent.ActionResult   { return nil }
func (f *fakeAgent) StopAll() []agent.ActionResult    { return nil }
func (f *fakeAgent) RestartAll() []agent.ActionResult { return nil }

func (f *fakeAgent) TailLog(service string, lines int) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeAgent) NodeCounters(nodeID int) (*agent.CounterData, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeAgent) InstallArtifact(spec agent.ArtifactSpec, content io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, err := io.Copy(io.Discard, content); err != nil {
		return err
	}
	f.installed = append(f.installed, spec)
	return nil
}

func (f *fakeAgent) Subscribe(buf int) (<-chan agent.Event, func()) {
	ch := make(chan agent.Event, buf)
	return ch, func() { close(ch) }
}

func (f *fakeAgent) Close() {}
