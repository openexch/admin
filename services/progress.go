// SPDX-License-Identifier: Apache-2.0
package services

import (
	"sync"
	"time"

	"github.com/openexch/admin/logging"
)

// Progress tracks long-running operation status
type Progress struct {
	mu          sync.RWMutex
	Operation   string    `json:"operation,omitempty"`
	OperationID string    `json:"opId,omitempty"` // correlation id, matches op_id in the logs
	CurrentStep int       `json:"currentStep"`
	TotalSteps  int       `json:"totalSteps"`
	Status      string    `json:"status,omitempty"`
	Complete    bool      `json:"complete"`
	Error       bool      `json:"error"`
	StartTime   time.Time `json:"-"`
	ElapsedMs   int64     `json:"elapsedMs,omitempty"`
}

func NewProgress() *Progress {
	return &Progress{}
}

// TryStart atomically claims the progress slot for an operation, returning
// false when another operation is still running. The check and the claim used
// to be separate (IsRunning() in the caller, Start() inside the spawned
// goroutine), so two operations could slip past each other and share the slot
// — found by the #10 chaos soak, where an auto-snapshot ran concurrently with
// a rolling update (nodes stopping while a snapshot propagates: the match#35
// hazard) and their statuses cross-attributed.
func (p *Progress) TryStart(operation string, totalSteps int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Operation != "" && !p.Complete {
		return false
	}
	p.Operation = operation
	p.OperationID = logging.NewOpID(operation)
	p.TotalSteps = totalSteps
	p.CurrentStep = 0
	p.Status = ""
	p.Complete = false
	p.Error = false
	p.StartTime = time.Now()
	return true
}

// CurrentOpID returns the correlation id minted by the last successful
// TryStart. The claimer reads it right after claiming the slot; no other
// operation can start until this one completes, so the value is stable.
func (p *Progress) CurrentOpID() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.OperationID
}

// CurrentOperation returns the name of the operation holding the slot (empty
// when idle). Read by recoverOp to attribute a panic to the op that died.
func (p *Progress) CurrentOperation() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Operation
}

func (p *Progress) Update(step int, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.CurrentStep = step
	p.Status = status
}

func (p *Progress) Finish(success bool, status string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Complete = true
	p.Error = !success
	p.Status = status
	p.CurrentStep = p.TotalSteps
}

func (p *Progress) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Operation = ""
	p.OperationID = ""
	p.CurrentStep = 0
	p.TotalSteps = 0
	p.Status = ""
	p.Complete = false
	p.Error = false
}

func (p *Progress) IsRunning() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Operation != "" && !p.Complete
}

func (p *Progress) ToMap() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := map[string]interface{}{
		"currentStep": p.CurrentStep,
		"totalSteps":  p.TotalSteps,
		"complete":    p.Complete,
		"error":       p.Error,
	}

	if p.Operation != "" {
		result["operation"] = p.Operation
	}
	if p.OperationID != "" {
		result["opId"] = p.OperationID
	}
	if p.Status != "" {
		result["status"] = p.Status
	}
	if !p.StartTime.IsZero() {
		result["elapsedMs"] = time.Since(p.StartTime).Milliseconds()
	}
	if p.TotalSteps > 0 {
		result["progress"] = int(float64(p.CurrentStep) / float64(p.TotalSteps) * 100)
	}

	return result
}
