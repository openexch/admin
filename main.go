// SPDX-License-Identifier: Apache-2.0
package main

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/openexch/admin/agent"
	"github.com/openexch/admin/config"
	"github.com/openexch/admin/handlers"
	"github.com/openexch/admin/logging"
	"github.com/openexch/admin/services"
)

func main() {
	// Load configuration
	cfg := config.Load()
	logging.Setup(cfg.LogFormat)

	// Active runtime profile (config/profiles.go): drives the service catalog's
	// heaps, idle strategies and pinning, and the preflight memory gate. The
	// knobs take effect as services (re)start; nothing here writes to the host.
	slog.Info("active runtime profile",
		"profile", cfg.ProfileName, "nodeHeapMB", cfg.Profile.NodeHeapMB,
		"idle", cfg.Profile.IdleMode, "driver", cfg.Profile.DriverProfile,
		"pinning", cfg.Profile.Pinning, "minMemMB", cfg.Profile.MinMemMB)

	// Initialize services
	// Cluster descriptors: the matching engine (existing) + the assets engine. Every
	// management op runs against one of these via the same code path. The existing
	// singletons stay bound to the matching engine so its behavior is unchanged;
	// the assets descriptor is registered for cluster-scoped management.
	matchCluster := services.NewMatchCluster(cfg)
	assetsCluster := services.NewAssetsCluster(cfg)
	cluster := matchCluster
	progress := services.NewProgress()
	clusterStatus := services.NewClusterStatusSized(matchCluster.NodeCount())
	// The in-process LocalAgent. Everything downstream depends only on the
	// agent contract (docs/AGENT-ARCHITECTURE.md), so a remote agentd client
	// can slot in per host later.
	var procMgr agent.ProcessAgent = services.NewProcessManager(cfg)

	statusSvc := services.NewStatusService(cfg, cluster, clusterStatus)
	statusSvc.SetProcessManager(procMgr)
	opsSvc := services.NewOperationsService(cfg, cluster, progress, clusterStatus)
	opsSvc.SetProcessManager(procMgr)
	opsSvc.SetStatusService(statusSvc)

	// Assets Engine ops: the SAME OperationsService code, bound to the assets
	// descriptor + its own status tracker. It shares the preflight instance so its
	// destructive ops are gated by the GLOBAL, box-wide invariants (mem-available,
	// disk-space) exactly like the matching engine's — starting an assets rolling
	// update on an OOM/full box is just as dangerous (ag#83). SetPreflight is wired
	// below, after preflight is constructed. The match-SPECIFIC checks
	// (cluster-quorum over the 3-node ME, driver-dirs over the ME's external
	// drivers) are scoped out for the assets cluster by Preflight.GateForCluster,
	// so a single-node embedded cluster is never gated on the ME's quorum.
	assetsClusterStatus := services.NewClusterStatusSized(assetsCluster.NodeCount())
	assetsOps := services.NewOperationsService(cfg, assetsCluster, progress, assetsClusterStatus)
	assetsOps.SetProcessManager(procMgr)
	// The AE reclaims its archive too, so it needs the same purge guard — scoped
	// to its OWN nodes (archiveOpBlockReason reads this cluster's block, not the
	// matching engine's). Without this its housekeeping would purge unguarded and
	// strand any member that was down at snapshot time.
	assetsOps.SetStatusService(statusSvc)
	// Each cluster's cleanup must never touch the other's state/driver dirs.
	opsSvc.SetPeerClusters([]*services.Cluster{assetsCluster})
	assetsOps.SetPeerClusters([]*services.Cluster{matchCluster})
	// Surface the AE as a first-class cluster in /status, sharing the assets ops'
	// transitional-state tracker so node stops/starts show STOPPING/STARTING.
	statusSvc.SetAssetsCluster(assetsCluster, assetsClusterStatus)
	clusterOps := map[string]*services.OperationsService{
		matchCluster.Name:  opsSvc,
		assetsCluster.Name: assetsOps,
	}
	preflight := services.NewPreflight(cfg)
	preflight.SetProcessManager(procMgr)
	preflight.SetStatusService(statusSvc)
	statusSvc.SetPreflight(preflight)
	opsSvc.SetPreflight(preflight)
	// Gate the assets cluster's destructive ops too (ag#83). The shared preflight
	// scopes out the match-specific checks for the assets cluster (see
	// Preflight.GateForCluster); the global mem/disk gates still apply.
	assetsOps.SetPreflight(preflight)
	// EVERY cluster, not just the matching engine: an unsnapshotted cluster log
	// grows without bound until the archive cannot write and its nodes die.
	autoSnapshot := services.NewAutoSnapshot(clusterOps)
	statusSvc.SetAutoSnapshot(autoSnapshot)
	autoSnapshot.Start(5) // Auto-snapshot every 5 minutes to prevent unbounded log growth
	logSvc := services.NewLogService(cfg)
	metricsSvc := services.NewMetricsService(statusSvc, opsSvc, procMgr, progress, preflight)

	// Cluster transitions (elections, node loss, quorum) for EVERY cluster. The
	// match gateway republishes its own on the market feed, which is why this
	// log used to live in the trading UI; that path has never covered the
	// Assets Engine, so the money ledger's elections were visible to nobody.
	clusterEvents := services.NewClusterEventLog()
	statusSvc.SetClusterEvents(clusterEvents)

	// Retention watermark, per cluster. Computed and reported always; whether it
	// bounds the purge is WATERMARK_RETENTION.
	for _, ops := range clusterOps {
		ops.SetWatermarkTracker(services.NewWatermarkTracker())
	}

	// Initialize handlers
	h := handlers.New(statusSvc, opsSvc, clusterOps, cluster, progress, clusterStatus, autoSnapshot, logSvc, procMgr, metricsSvc, preflight, cfg)
	h.SetClusterEvents(clusterEvents)

	// Setup router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(logging.RequestLogger)
	r.Use(metricsSvc.Middleware)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(handlers.AuthMiddleware(cfg.AuthToken))

	h.RegisterRoutes(r)

	// Secure-by-default (admin-gateway#11): loopback bind unless overridden;
	// a non-loopback bind without a token would expose every destructive op,
	// so refuse to start in that combination.
	if cfg.AuthToken == "" {
		if ip := net.ParseIP(cfg.BindAddr); ip == nil || !ip.IsLoopback() {
			slog.Error("refusing to bind without an auth token: set ADMIN_AUTH_TOKEN(_FILE) or bind loopback (ADMIN_BIND=127.0.0.1)",
				"bind", cfg.BindAddr)
			os.Exit(1)
		}
		slog.Warn("no admin token configured, loopback-only dev mode")
	}

	// Start server
	addr := cfg.BindAddr + ":" + cfg.Port
	slog.Info("admin gateway starting",
		"addr", addr, "project", cfg.ProjectDir, "jar", cfg.JarPath)

	// Graceful shutdown
	server := &http.Server{
		Addr:    addr,
		Handler: r,
	}

	// Desired-state reconcile: restore the operator's last intent after a
	// reboot or an admin-gateway restart. Runs in the background so a slow
	// stack bring-up never blocks the gateway from serving. Local-agent only
	// (the concrete ProcessManager owns the desired-state file); a remote
	// agentd would reconcile on its own host.
	if pm, ok := procMgr.(*services.ProcessManager); ok {
		go pm.ReconcileDesired()
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		slog.Info("shutting down")
		autoSnapshot.Stop()
		procMgr.Close()
		statusSvc.Stop()
		server.Close()
	}()

	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}
