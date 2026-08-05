// SPDX-License-Identifier: Apache-2.0
package services

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

// Op-agnostic durable failure record (ag#67).
//
// Every long-running operation runs in a `go o.doX()` goroutine and reports
// only to the in-memory Progress singleton. If that goroutine dies — a panic
// (which in Go is fatal to the WHOLE process) or an admin restart mid-op — the
// transient Progress status vanishes with it and the shared slot frees silently,
// so the operator never learns WHY (the "rebuild-gateway dies silently" symptom
// in ag#67). rebuild-admin already had a durable record (rebuild_verify.go), but
// it was wired only to that one op via failRebuild.
//
// This generalizes the same durability to ALL ops: recoverOp (deferred at the
// top of every doX goroutine) contains a panic, records it durably to
// op-failure.json, and Finishes the Progress slot with the error so it frees
// cleanly instead of wedging every future operation. The record is queryable
// after the fact via GET /api/admin/last-op-failure (LastOpFailure).
//
// It reuses writeJSONAtomic (rebuild_verify.go) and is deliberately SEPARATE
// from rebuild-result.json: that file is the rebuild-admin verification
// handshake with its own lifecycle, whereas this is a single "last op that died
// unexpectedly" record shared across every op.

const opFailureFile = "op-failure.json"

// opFailureRecord is the durable shape written when an op goroutine dies.
type opFailureRecord struct {
	Op     string `json:"op"`
	OpID   string `json:"opId"`
	Stage  string `json:"stage"` // "panic" today; a hook for future durable stages
	Reason string `json:"reason"`
	At     string `json:"at"`
}

// WriteOpFailure durably records that operation op (correlation opID) died at
// stage with reason, overwriting any previous record (only the LAST unexpected
// death is retained — the running op's live state is in Progress).
func WriteOpFailure(adminDir, op, opID, stage, reason string) error {
	if adminDir == "" {
		return fmt.Errorf("no admin dir configured for durable op-failure record")
	}
	return writeJSONAtomic(filepath.Join(adminDir, opFailureFile), opFailureRecord{
		Op:     op,
		OpID:   opID,
		Stage:  stage,
		Reason: reason,
		At:     time.Now().Format(time.RFC3339),
	})
}

// ReadOpFailure reports the last durable op-failure record for
// GET /api/admin/last-op-failure. state="none" when there is no record.
func ReadOpFailure(adminDir string) map[string]interface{} {
	data, err := os.ReadFile(filepath.Join(adminDir, opFailureFile))
	if err != nil {
		return map[string]interface{}{"state": "none"}
	}
	var rec opFailureRecord
	if json.Unmarshal(data, &rec) != nil {
		return map[string]interface{}{"state": "none"}
	}
	return map[string]interface{}{
		"state":  "failed",
		"op":     rec.Op,
		"opId":   rec.OpID,
		"stage":  rec.Stage,
		"reason": rec.Reason,
		"at":     rec.At,
	}
}

// LastOpFailure is the handler-facing accessor for the durable op-failure record.
func (o *OperationsService) LastOpFailure() map[string]interface{} {
	return ReadOpFailure(o.cfg.AdminDir)
}

// recoverOp is deferred at the top of every doX operation goroutine. On a panic
// it (1) contains it — an unrecovered panic in any goroutine is fatal to the
// whole admin gateway, which is itself the process managing a live trading
// cluster — (2) records it durably so it survives even a subsequent restart, and
// (3) Finishes the Progress slot with the error so the slot frees instead of
// wedging every future operation. On a normal return recover() is nil and this
// is a no-op, so it is safe to defer unconditionally (happy-path behavior is
// unchanged). Never re-panics: keeping the control plane alive is the safe choice.
func (o *OperationsService) recoverOp() {
	r := recover()
	if r == nil {
		return
	}
	stack := debug.Stack()
	op, opID := "", ""
	if o.progress != nil {
		op = o.progress.CurrentOperation()
		opID = o.progress.CurrentOpID()
	}
	reason := fmt.Sprintf("panic: %v", r)
	if o.log != nil {
		o.log.Error("operation goroutine panicked", "op", op, "op_id", opID,
			"reason", reason, "stack", string(stack))
	}
	if err := WriteOpFailure(o.cfg.AdminDir, op, opID, "panic", reason+"\n"+string(stack)); err != nil && o.log != nil {
		o.log.Error("could not persist durable op-failure record", "err", err)
	}
	if o.progress != nil {
		o.progress.Finish(false, "operation failed ("+reason+") — see GET /api/admin/last-op-failure")
	}
}
