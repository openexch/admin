// SPDX-License-Identifier: Apache-2.0
package services

import (
	"encoding/json"
	"os"
)

// writeJSONAtomic writes v as indented JSON via a temp file and a rename.
//
// The durable records this core keeps (the last op failure, desired service
// state) are read by a process that may have just come back from a crash. A
// half-written file there is worse than no file: it reads as corrupt state
// rather than as absence, and absence is the case every reader already handles.
func writeJSONAtomic(path string, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
