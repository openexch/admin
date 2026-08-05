.PHONY: build build-agentd run clean install uninstall reinstall rebuild processes processes-summary help proto loopback

# ==================== CONFIGURATION ====================
ADMIN_GATEWAY_DIR := $(shell pwd)
MATCH_PROJECT_DIR := $(ADMIN_GATEWAY_DIR)/../match
# Written into the unit explicitly rather than left to the binary's defaults.
# A box provisioned before the order-management -> oms rename keeps working by
# overriding these, instead of depending on which default the deployed binary
# happens to carry.
OMS_PROJECT_DIR := $(ADMIN_GATEWAY_DIR)/../oms
ASSETS_PROJECT_DIR := $(ADMIN_GATEWAY_DIR)/../assets
LOG_DIR := $(HOME)/.local/log/cluster
USER_SERVICE_DIR := $(HOME)/.config/systemd/user

LOG_ROTATE = /bin/bash -c '"'"'test -f $(LOG_DIR)/$(1).log && mv $(LOG_DIR)/$(1).log $(LOG_DIR)/$(1).log.$$(date +%%Y%%m%%d-%%H%%M%%S) || true'"'"'

# ==================== BUILD ====================

build:
	go build -o admin-gateway .

build-agentd:
	go build -o agentd-bin ./cmd/agentd

# End-to-end smoke of the REAL agentd binary against a TCP hub (the
# in-process loopback parity tests run in plain `go test ./...`).
loopback:
	go test -tags loopback ./agenthub/ -run TestAgentdBinarySmoke -v

# Regenerate agentwire/*.pb.go from agentwire/agent.proto. Pure-Go toolchain
# (buf compiles the proto; no C++ protoc). Generated code is COMMITTED — CI
# and normal builds never run this.
PROTOC_GEN_GO_VERSION      := v1.36.10
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1
BUF_VERSION                := v1.50.0
TOOLS_DIR := $(ADMIN_GATEWAY_DIR)/.tools

proto:
	GOBIN=$(TOOLS_DIR) go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	GOBIN=$(TOOLS_DIR) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION) generate --path agentwire/agent.proto

run: build
	./admin-gateway

clean:
	rm -f admin-gateway

# ==================== SERVICE MANAGEMENT ====================

install: build
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║     Installing Admin Gateway Service (Process Manager)           ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  Admin gateway is the process manager for all services."
	@echo "  Only admin.service uses systemd. All other processes are"
	@echo "  managed directly by the admin gateway process manager."
	@echo ""
	@mkdir -p $(USER_SERVICE_DIR)
	@mkdir -p $(LOG_DIR)
	@mkdir -p $(HOME)/.local/run/match
	@echo "→ Removing old per-service systemd units (if any)..."
	@systemctl --user stop node0 node1 node2 backup order market 2>/dev/null || true
	@systemctl --user disable node0 node1 node2 backup order market 2>/dev/null || true
	@rm -f $(USER_SERVICE_DIR)/node0.service $(USER_SERVICE_DIR)/node1.service $(USER_SERVICE_DIR)/node2.service
	@rm -f $(USER_SERVICE_DIR)/backup.service $(USER_SERVICE_DIR)/market.service
	@rm -f $(USER_SERVICE_DIR)/order.service
	@echo "→ Installing admin.service (Go process manager)..."
	@printf '%s\n' \
		'[Unit]' \
		'Description=Match Engine Admin Gateway + Process Manager' \
		'After=default.target' \
		'' \
		'[Service]' \
		'Type=simple' \
		'WorkingDirectory=$(MATCH_PROJECT_DIR)' \
		'Environment="MATCH_PROJECT_DIR=$(MATCH_PROJECT_DIR)"' \
		'Environment="OMS_PROJECT_DIR=$(OMS_PROJECT_DIR)"' \
		'Environment="ASSETS_PROJECT_DIR=$(ASSETS_PROJECT_DIR)"' \
		'ExecStartPre=$(call LOG_ROTATE,admin)' \
		'ExecStart=$(ADMIN_GATEWAY_DIR)/admin-gateway' \
		'Restart=on-failure' \
		'RestartSec=5' \
		'TimeoutStopSec=5' \
		'KillMode=process' \
		'StandardOutput=append:$(LOG_DIR)/admin.log' \
		'StandardError=append:$(LOG_DIR)/admin.log' \
		'' \
		'[Install]' \
		'WantedBy=default.target' > $(USER_SERVICE_DIR)/admin.service
	@echo ""
	@echo "→ Reloading user systemd..."
	@systemctl --user daemon-reload
	@echo "→ Enabling admin service..."
	@systemctl --user enable admin
	@echo ""
	@echo "→ Starting admin gateway..."
	@systemctl --user start admin
	@sleep 2
	@curl -sf -X POST http://localhost:8082/api/admin/processes/start-all > /dev/null
	@echo "  Process manager starting all services in dependency order..."
	@sleep 15
	@sleep 3
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║  ✓ Admin gateway installed and running!                          ║"
	@echo "║                                                                  ║"
	@echo "║  Order API:      http://localhost:8080/order                     ║"
	@echo "║  Market WS:      ws://localhost:8081/ws                          ║"
	@echo "║  Admin API:      http://localhost:8082/api/admin/status          ║"
	@echo "║                                                                  ║"
	@echo "║  Process control:  make processes                                ║"
	@echo "║  Start everything: curl -X POST .../processes/start-all         ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"

uninstall:
	@echo "→ Stopping all processes via admin..."
	@curl -sf -X POST http://localhost:8082/api/admin/processes/stop-all 2>/dev/null || true
	@sleep 5
	@echo "→ Stopping admin gateway..."
	@systemctl --user stop admin 2>/dev/null || true
	@systemctl --user disable admin 2>/dev/null || true
	@rm -f $(USER_SERVICE_DIR)/admin.service
	@echo "→ Cleaning up old service files..."
	@rm -f $(USER_SERVICE_DIR)/node0.service $(USER_SERVICE_DIR)/node1.service $(USER_SERVICE_DIR)/node2.service
	@rm -f $(USER_SERVICE_DIR)/backup.service $(USER_SERVICE_DIR)/market.service
	@rm -f $(USER_SERVICE_DIR)/order.service
	@systemctl --user daemon-reload
	@echo "→ Cleaning PID files..."
	@rm -f $(HOME)/.local/run/match/*.pid
	@echo "✓ All services uninstalled"

reinstall: uninstall install
	@echo ""
	@echo "✓ Services reinstalled"

# ==================== RUNTIME ====================

rebuild:
	@echo "→ Triggering admin gateway self-update..."
	@curl -sf -X POST http://localhost:8082/api/admin/rebuild-admin | python3 -m json.tool 2>/dev/null || echo '{"error": "admin gateway not running"}'
	@echo "Admin gateway will rebuild and restart automatically"

processes:
	@curl -sf http://localhost:8082/api/admin/processes 2>/dev/null | python3 -c "import sys,json;data=json.load(sys.stdin);[print(f\"  {'●' if p['running'] else '○'} {p['name']:10s} {p['status']:10s} PID {str(p.get('pid') or '-'):>8s}  {p.get('memoryBytes',0)//1048576:>5d} MB  {p.get('cpuPercent',0):>6.1f}%%\") for p in data]" 2>/dev/null || echo "  Admin gateway not running"

processes-summary:
	@curl -sf http://localhost:8082/api/admin/processes/summary 2>/dev/null | python3 -m json.tool 2>/dev/null || echo '{"error": "admin gateway not running"}'

# ==================== HELP ====================

help:
	@echo "Admin Gateway — Process Manager for Open Exchange"
	@echo ""
	@echo "Build:"
	@echo "  make build              Build the binary"
	@echo "  make run                Build and run"
	@echo "  make clean              Remove binary"
	@echo ""
	@echo "Service Management:"
	@echo "  make install            Build, install systemd service, and start"
	@echo "  make uninstall          Stop all and remove services"
	@echo "  make reinstall          Uninstall + install"
	@echo "  make rebuild            Self-update via API (hot reload)"
	@echo ""
	@echo "Process Manager:"
	@echo "  make processes          Show live status of all processes"
	@echo "  make processes-summary  Process summary (running/stopped/memory)"
	@echo ""
	@echo "Runtime API: http://localhost:8082/api/admin/"
	@echo "  GET  .../processes                  Live process status"
	@echo "  POST .../processes/{name}/start     Start a service"
	@echo "  POST .../processes/{name}/stop      Stop a service"
	@echo "  POST .../processes/start-all        Start all (dependency order)"
	@echo "  POST .../processes/stop-all         Stop all (reverse order)"
	@echo "  POST .../rolling-update             Deploy code (zero-downtime)"
	@echo "  POST .../snapshot                   Take cluster snapshot"
	@echo "  POST .../rebuild-admin              Self-update admin gateway"
