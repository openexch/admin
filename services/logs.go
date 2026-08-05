// SPDX-License-Identifier: Apache-2.0
package services

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/openexch/admin/config"
)

// LogService reads log files from the cluster log directory
type LogService struct {
	logDir string
}

func NewLogService(cfg *config.Config) *LogService {
	return &LogService{logDir: cfg.LogDir}
}

// GetNodeLogs returns the last N lines from a matching-engine node's log file.
func (l *LogService) GetNodeLogs(nodeId, lines int) map[string]interface{} {
	return l.GetNodeLogsNamed(fmt.Sprintf("node%d", nodeId), nodeId, lines)
}

// GetNodeLogsNamed returns a cluster node's log by its process-manager name
// (node0.. for the matching engine, ae0 for the assets engine), so the log
// viewer resolves the right file per cluster. nodeId is echoed for the client.
func (l *LogService) GetNodeLogsNamed(name string, nodeId, lines int) map[string]interface{} {
	return l.getLogFile(name, lines, map[string]interface{}{"node": nodeId})
}

// GetServiceLogs returns the last N lines from a service's log file
func (l *LogService) GetServiceLogs(service string, lines int) map[string]interface{} {
	// Map service names to log file names
	logName := service
	switch service {
	case "market-gateway":
		logName = "market"
	case "oms", "order-management":
		logName = "oms"
	case "admin-gateway":
		logName = "admin"
	}

	return l.getLogFile(logName, lines, map[string]interface{}{"service": service})
}

func (l *LogService) getLogFile(name string, lines int, extra map[string]interface{}) map[string]interface{} {
	result := map[string]interface{}{}
	for k, v := range extra {
		result[k] = v
	}

	logPath := filepath.Join(l.logDir, name+".log")
	file, err := os.Open(logPath)
	if err != nil {
		result["logs"] = []string{}
		result["error"] = "Log file not found: " + logPath
		return result
	}
	defer file.Close()

	// Read all lines (efficient enough for log files)
	var allLines []string
	scanner := bufio.NewScanner(file)
	// Increase buffer for long lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		allLines = append(allLines, scanner.Text())
	}

	// Cap lines at 500
	if lines > 500 {
		lines = 500
	}
	// Floor at 0: a negative count would make start exceed len(allLines) and
	// panic the slice below (the start<0 guard alone does not cover it).
	if lines < 0 {
		lines = 0
	}

	start := len(allLines) - lines
	if start < 0 {
		start = 0
	}

	result["logs"] = allLines[start:]
	result["totalLines"] = len(allLines)
	return result
}
