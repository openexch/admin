# Operations runbooks

Task-oriented procedures for running the Open Exchange stack through the admin
gateway. Every runtime operation goes through `http://127.0.0.1:8082/api/admin/*`
(never raw `systemctl`/`kill` unless a runbook says so explicitly).

Conventions used below:

```bash
ADMIN=http://127.0.0.1:8082
# If ADMIN_AUTH_TOKEN is configured, add to every curl:
#   -H "Authorization: Bearer $ADMIN_AUTH_TOKEN"
```

- Long-running operations return `202` and report through `GET /api/admin/progress`.
  The response carries `opId`; every log line of that operation carries the same
  `op_id` (`grep <opId> ~/.local/log/cluster/admin.log`).
- `GET /api/admin/status` is the health source of truth: per-node
  `health`/`pidAlive`/`cncRole`/`commitAdvancing`, `allNodesHealthy`, `leader`,
  `backup.fresh`.
- Logs: the admin gateway writes structured JSON to
  `~/.local/log/cluster/admin.log` (the systemd unit redirects stdout there;
  the journal only has systemd's own lines). Managed processes log to
  `~/.local/log/cluster/<name>.log` (also `GET /api/admin/logs?node=N&lines=100`).

Related documents: `match/docs/backup-restore.md` (backup architecture, RPO/RTO),
`match/docs/incidents/` (post-mortems behind several rules here),
`match/docs/kernel-bypass.md` (transport/driver reference).

---

## 1. Start / stop the stack

### Start (cold box)

1. After a reboot or kernel upgrade, verify OS tuning first (in `match/`):
   `make tune-report` must show no drift. Unpersisted socket limits crash-loop
   the media drivers and can corrupt archives (match#48). If drift:
   `sudo make tune-persist`, re-check.
2. Ensure the admin gateway itself is up:
   `systemctl --user start admin` (the only systemd-managed piece), then
   `curl $ADMIN/health`.
3. Start everything in dependency order (drivers, then nodes, then apps):

   ```bash
   curl -X POST $ADMIN/api/admin/processes/start-all
   ```

   The driver gate holds each nodeN until driverN has been up 5 s and its
   CnC file exists; rapid crash-looping (5 crashes / 2 min) parks a service as
   `failed` with `lastError` set. An explicit start re-arms it.
4. Verify:
   - `status`: 3/3 nodes healthy, one `LEADER`, `allNodesHealthy: true`,
     `backup.fresh: true`.
   - OMS log line `PostgreSQL persistence initialized` (NOT "running without
     persistence"), and its startup rebuild/reconcile completed.
   - Submit a test order end-to-end if in doubt.

### Stop

```bash
curl -X POST $ADMIN/api/admin/processes/stop-all   # reverse dependency order
```

Then `systemctl --user stop admin` only if the gateway itself must go down.
Archives live on tmpfs: a machine power-off after stop-all loses cluster state
by design, durability is the disk backup plus the OMS Postgres ledger (see
runbook 3).

### Known quirk: node0 limbo after abrupt stops

Symptom: node0 shows DEGRADED, JVM alive but agents dead, CnC unreadable
(ShutdownSignalBarrier limbo). Reliable fix:

```bash
curl -X POST $ADMIN/api/admin/processes/node0/force-stop
curl -X POST $ADMIN/api/admin/processes/driver0/restart
curl -X POST $ADMIN/api/admin/processes/node0/start
```

The same sequence applies if `/dev/shm/aeron-emre-0-driver` vanishes under a
live driver0.

Since the #42 guards, the "dir vanishes under a live driver" state should no
longer be reachable through the admin API: every deleter checks the launch
script's `<dir>.pid` ground truth (not just tracked state), a driver start
while an untracked orphan `aeronmd` holds the dir is refused with one clear
error (instead of burning the crash-loop cap on idempotent exit-0 launches),
and `force-stop driverN` kills the orphan pid too. If the invariant does
fire, `/api/admin/status` invariants and `admin_invariant_ok{check="driver-dirs"}`
flag it; recover with the sequence above (force-stop now clears the orphan).

---

## 2. Snapshot, backup, restore

### Snapshot (manual)

```bash
curl -X POST $ADMIN/api/admin/snapshot
```

- Auto-snapshot runs on an interval (`GET/POST/DELETE /api/admin/auto-snapshot`).
- The snapshot operation refuses (409, names the unhealthy node) unless
  `allNodesHealthy`. `{"force":true}` overrides for drills only; auto-snapshot
  never forces. This guard exists because snapshotting with a lagging member
  strands it permanently (match#35).
- After a successful snapshot, live archive housekeeping reclaims disk
  automatically; standalone: `POST /api/admin/housekeeping` (same health guard).

### Backup freshness (continuous)

```bash
curl $ADMIN/api/admin/backup-info     # fresh, freshReason, heartbeat detail
```

Alert on `status.backup.fresh == false`, never on `running` alone: a running
backup process proves nothing (match#36, the agent wedged silently for days).
The backup app self-halts on stall and the process manager restarts it.

### Restore

Full procedures with copy semantics and verification live in
`match/docs/backup-restore.md`. Summary:

- Single corrupted/stranded node, quorum intact: prefer reseeding from a healthy
  follower (runbook 4); otherwise `force-stop nodeN`, then
  `POST /api/admin/recover-from-backup {"nodeId":N,"force":true}`, then start.
- Full cluster after power loss (/dev/shm wiped): recover-from-backup for each
  node 0..2 while everything is stopped, then `processes/start-all`, verify
  3/3 consensus, then restart the backup service last. Validated by the
  2026-07-04 power-loss drill (`match/tools/durability/backup_restore_drill.sh`),
  measured RPO ≈ 0 with the OMS reconciling cleanly.

---

## 3. Cleanup (IPC dirs, mark files, archives)

`POST /api/admin/cleanup` requires all nodes and drivers stopped, and PRESERVES
archives by default:

```bash
curl -X POST $ADMIN/api/admin/cleanup -d '{"dryRun":true}'   # allowed anytime
curl -X POST $ADMIN/api/admin/cleanup -d '{"force":true}'    # IPC + marks, archives kept
```

A full state wipe (destroys cluster history; the disk backup and Postgres
ledger are then the only copies) additionally requires the explicit triple:

```bash
curl -X POST $ADMIN/api/admin/cleanup \
  -d '{"force":true,"includeArchive":true,"confirmArchiveLoss":"DELETE-CLUSTER-STATE"}'
```

Never delete `/dev/shm/aeron-*` by hand: the glob also matches live driver
dirs and `aeron-cluster` (this exact mistake corrupted a cluster before the
guardrails). `cleanup-node` does the same per node.

---

## 4. Leader stuck / node stranded

### Leader present but cluster not serving

Symptoms: `status` shows a leader yet orders do not flow (gateway logs
`AWAIT_PUBLICATION_CONNECTED`, UI order book frozen).

1. Cross-check liveness, not just status: `ss -uln` for the ingress ports,
   fresh lines in node logs. The status API can serve stale data for dead
   processes in edge cases (admin-gateway#13 history).
2. Restart the market gateway first (cheapest):
   `POST /api/admin/processes/market/restart`.
3. If ingress still dead, restart followers one at a time, leader last
   (a binary change is a job for the layer that deploys, not for this core).

### No leader elected / election loop

1. `GET /api/admin/logs?node=N` on each node: look for repeated identical
   Aeron errors. 200 consecutive byte-identical errors halt the node
   deliberately (replay hot-loop fail-fast, match#55) so the PM can restart
   it clean; a node crash-looping into `failed` state means its archive is
   likely corrupt: treat as stranded member below.
2. With 2/3 healthy, quorum recovers on its own. Do NOT snapshot or housekeep
   until it does (the guard enforces this).

### Stranded/corrupt member (rejoin loops, "does not point to a valid frame")

Reseed from a healthy follower (automated; the validated match#35 procedure):

```bash
curl -X POST $ADMIN/api/admin/reseed-node \
  -H 'Content-Type: application/json' \
  -d '{"nodeId": <stranded>, "sourceNodeId": <healthy follower>, "force": true}'
```

`force` acknowledges the cost: the source follower stops for the copy, so the
cluster loses quorum (ingress stalls) for those seconds. The operation refuses
a leader as source or target, restarts the source on every failure path, and
finishes only when both members confirm catch-up by commit position. Poll
`/api/admin/progress`.

Manual fallback (what the endpoint does, for when the gateway itself is down):

1. Stop the stranded node and one HEALTHY FOLLOWER (never the leader).
2. Copy the follower's `cluster/` + `archive/` dirs over the stranded node's
   wiped state, EXCLUDING `cluster-mark*.dat`, `node-state.dat`,
   `archive-mark.dat`, `*.lck`.
3. Start both; the reseeded member rejoins at the leader's commit position.

If no healthy source exists, use the disk backup (runbook 3).

---

## 5. Disk full

Two distinct disks can fill:

### tmpfs `/dev/shm` (live archives, ~165 B/order/node)

16 GB fills in under a minute at max load. A full shm wedges followers AND
breaks ClusterTool/admin snapshot operations, so act before 100%.

1. Check: `df -h /dev/shm`.
2. Reclaim: `POST /api/admin/snapshot` (housekeeping purges log segments below
   the snapshot; requires all nodes healthy). Between heavy load runs, always
   housekeep.
3. If a node already wedged on ENOSPC: housekeep the healthy nodes, then treat
   the wedged one as stranded (runbook 4). Do not snapshot while it is down.

### Real disk (backup dir, Postgres, logs)

1. Check: `df -h ~`, biggest consumers are `match/backup/` (bounded by
   housekeeping on the source side but verify), old `*.pre-reset-*` backup
   copies (safe to delete after confirming a fresh backup), Postgres, and
   `~/.local/log/cluster/`.
2. The backup app stalls (and reports `fresh:false`) on a full disk; free space
   and it recovers on the next PM restart cycle.

---
