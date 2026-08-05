// SPDX-License-Identifier: Apache-2.0
import { useState, useEffect, useCallback, useRef } from 'react';
import { useTheme } from '../hooks/useTheme';
import { ThemeToggle } from '../components/ThemeToggle/ThemeToggle';
import { LogoMark } from '../components/LogoMark';
import { Icons } from '../components/Icons';
import { RiskAdmin } from '../components/admin/RiskAdmin';
import { BackupOps } from '../components/admin/BackupOps';
import { EventFeed } from '../components/admin/EventFeed';
import { ConfirmModal } from '../components/admin/ConfirmModal';
import { ToastProvider, useToast } from '../components/admin/Toasts';
import { ClusterSection, ClusterSkeleton } from '../components/admin/ClusterSection';
import { OverviewDashboard } from '../components/admin/OverviewDashboard';
import { ServicesSection } from '../components/admin/ServicesSection';
import { LogViewer } from '../components/admin/LogViewer';
import { adminUrl, normalizeStatus } from '../components/admin/api';
import { GRAFANA_URL, TRADING_URL } from '../config';
import { useAdminEvents, type AdminProgress } from '../hooks/useAdminEvents';
import type {
  AdminStatus,
  AdminTab,
  ConfirmAction,
  LogSource,
  ProcessInfo,
  ProcessSummary,
} from '../components/admin/types';

export function AdminPage() {
  return (
    <ToastProvider>
      <AdminConsole />
    </ToastProvider>
  );
}

/**
 * Gateway connectivity — persistent state in a reserved-width pill, never a
 * banner (connectivity is not an event). live = REST + stream up;
 * degraded = REST up, stream down; down = REST unreachable.
 */
/** Fleet counts for the Overview strip, derived from the process list. */
function summarize(list: ProcessInfo[]): ProcessSummary {
  let running = 0, stopped = 0, failed = 0, memoryBytes = 0;
  for (const p of list) {
    if (p.running) {
      running++;
      memoryBytes += p.memoryBytes ?? 0;
    } else if (p.status === 'failed' || p.status === 'crashed') {
      failed++;
    } else {
      stopped++;
    }
  }
  return {
    total: list.length,
    running,
    stopped,
    failed,
    totalMemoryMB: Math.round(memoryBytes / (1024 * 1024)),
    lastPollMs: 0,
  };
}

function GatewayIndicator({ gatewayOk, eventsConnected }: { gatewayOk: boolean; eventsConnected: boolean }) {
  const state = !gatewayOk ? 'down' : eventsConnected ? 'live' : 'degraded';
  const DOT: Record<string, string> = {
    live: 'bg-buy',
    degraded: 'bg-warn animate-pulse-soft',
    down: 'bg-sell animate-pulse-soft',
  };
  return (
    <span
      title={`Admin gateway: ${state === 'live' ? 'connected' : state === 'degraded' ? 'connected, event stream down' : 'unreachable'}`}
      className="flex w-[110px] flex-shrink-0 items-center justify-end gap-1.5 text-[11px] font-medium text-muted"
    >
      <span className={`h-1.5 w-1.5 flex-shrink-0 rounded-full ${DOT[state]}`} />
      <span className="tabular-nums">{state === 'live' ? 'Gateway' : state === 'degraded' ? 'Degraded' : 'Offline'}</span>
    </span>
  );
}

function AdminConsole() {
  const { theme, toggle } = useTheme();
  const toast = useToast();
  const [tab, setTab] = useState<AdminTab>('overview');
  const [status, setStatus] = useState<AdminStatus | null>(null);
  // REST reachability: connectivity is persistent state (the header pill),
  // never a banner or a per-poll toast.
  const [gatewayOk, setGatewayOk] = useState(true);
  const [progress, setProgress] = useState<AdminProgress | null>(null);
  // Which cluster the (single, shared) backend progress record belongs to.
  // The backend Progress has no cluster field — only one op runs at a time
  // across ALL clusters — so we attribute it client-side.
  const [activeOpCluster, setActiveOpCluster] = useState<string | null>(null);
  // null = never loaded (skeletons); [] = loaded-and-empty (quiet notice).
  const [processes, setProcesses] = useState<ProcessInfo[] | null>(null);
  const [logsUnavailable, setLogsUnavailable] = useState(false);
  const [processSummary, setProcessSummary] = useState<ProcessSummary | null>(null);
  const [operatingServices, setOperatingServices] = useState<Set<string>>(new Set());
  // Per-cluster snapshot-button busy (5s blind window), keyed by cluster name.
  const [snapshotOps, setSnapshotOps] = useState<Set<string>>(new Set());
  const [logSource, setLogSource] = useState<LogSource | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [pendingAction, setPendingAction] = useState<ConfirmAction | null>(null);
  const [feedOpen, setFeedOpen] = useState(false);
  // The generic clusters[] the whole console renders. normalizeStatus makes
  // this work against BOTH the new (clusters[]) and legacy (flat) backend and
  // guarantees ≥1 block once status has loaded.
  const clusters = status ? normalizeStatus(status) : null;
  const clusterDisplayOf = useCallback(
    (name: string) => clusters?.find(c => c.name === name)?.display ?? name,
    [clusters],
  );

  // GLOBAL lock: any op running anywhere disables every mutating button and
  // the ProfileSelector. Only ONE backend operation runs at a time.
  const stackBusy = !!(progress?.operation && !progress.complete);

  const fetchStatus = useCallback(async () => {
    try {
      const response = await fetch(adminUrl('/api/admin/status'));
      if (response.ok) {
        const data = await response.json() as AdminStatus;
        setStatus(data);
        setGatewayOk(true);
      } else {
        setGatewayOk(false);
      }
    } catch {
      // Keep the last-good data on screen; the pill carries the bad news.
      setGatewayOk(false);
    }
  }, []);

  const fetchProgress = useCallback(async () => {
    try {
      const response = await fetch(adminUrl('/api/admin/progress'));
      if (response.ok) {
        const data = await response.json() as AdminProgress;
        if (data.operation || data.currentStep > 0) {
          setProgress(data);
          if (data.complete) {
            setTimeout(async () => {
              await fetch(adminUrl('/api/admin/progress?reset=true'));
              setProgress(null);
              setActiveOpCluster(null);
            }, 3000);
          }
        } else {
          // The server-side record is gone (reset). If we still hold an
          // incomplete operation, it's stale — clear it or the rail sticks
          // mid-percent and every action stays disabled until a refresh.
          setProgress(prev => (prev && !prev.complete ? null : prev));
        }
      }
    } catch {
      // Ignore
    }
  }, []);

  const fetchLogs = useCallback(async () => {
    if (!logSource) return;
    try {
      let url = adminUrl('/api/admin/logs') + '?lines=200';
      if (logSource.type === 'node') {
        url += `&node=${logSource.id}&cluster=${encodeURIComponent(logSource.cluster)}`;
      } else {
        url += `&service=${logSource.name}`;
      }
      const response = await fetch(url);
      if (response.ok) {
        const data = await response.json();
        setLogs(data.logs || []);
        setLogsUnavailable(false);
      } else {
        setLogsUnavailable(true);
      }
    } catch {
      setLogsUnavailable(true);
    }
  }, [logSource]);

  // The fleet summary is DERIVED here, not fetched. It was a second endpoint
  // returning counts the process list already carries, and two sources for one
  // fact drift: a summary from a poll 200ms older than the list shows a service
  // as running that the list next to it shows as failed.
  const fetchProcesses = useCallback(async () => {
    try {
      const listRes = await fetch(adminUrl('/api/admin/processes'));
      if (!listRes.ok) {
        setGatewayOk(false);
        return;
      }
      const list: ProcessInfo[] = await listRes.json();
      setProcesses(list);
      setGatewayOk(true);
      setProcessSummary(summarize(list));
    } catch {
      setGatewayOk(false);
    }
  }, []);

  // Live event stream: process lifecycle events feed the Activity panel and
  // trigger an immediate process-list refresh; progress arrives pushed on
  // change, replacing the old 50ms HTTP fast-poll during operations.
  const {
    events: feedEntries,
    progress: sseProgress,
    connected: eventsConnected,
    unseen: feedUnseen,
    markSeen: markFeedSeen,
  } = useAdminEvents((ev) => {
    fetchProcesses();
    // A 'started' (or 'crashed') event is the real end of a start/restart —
    // clear the operating flag now instead of waiting out the blind timeout
    // (which stays as fallback). 'stopped' is deliberately NOT cleared here:
    // during a restart it would flash the card back to a stopped state.
    if (ev.type === 'started' || ev.type === 'crashed') {
      setOperatingServices(prev => {
        if (!prev.has(ev.service)) return prev;
        const next = new Set(prev);
        next.delete(ev.service);
        return next;
      });
    }
  });
  const eventsConnectedRef = useRef(eventsConnected);
  eventsConnectedRef.current = eventsConnected;
  const operationActiveRef = useRef(false);
  operationActiveRef.current = stackBusy;

  // Attribution catch-all: once no op is running, drop the cluster tag so a
  // later auto/unattributed op doesn't inherit a stale target.
  useEffect(() => {
    if (!stackBusy) setActiveOpCluster(null);
  }, [stackBusy]);

  // Events arriving while the panel is open are already "seen".
  useEffect(() => {
    if (feedOpen) markFeedSeen();
  }, [feedOpen, feedEntries, markFeedSeen]);

  useEffect(() => {
    if (!sseProgress) return;
    if (sseProgress.operation || sseProgress.currentStep > 0) {
      setProgress(sseProgress);
      if (sseProgress.complete) {
        setTimeout(async () => {
          await fetch(adminUrl('/api/admin/progress?reset=true'));
          setProgress(null);
          setActiveOpCluster(null);
        }, 3000);
      }
    } else {
      // Empty frame after a server-side reset: drop a stale incomplete op
      // (seen live: a snapshot frame stuck the rail at 71% until refresh).
      setProgress(prev => (prev && !prev.complete ? null : prev));
    }
  }, [sseProgress]);

  useEffect(() => {
    fetchStatus();
    fetchProgress();
    const interval = setInterval(() => {
      fetchStatus();
      // Progress rides the event stream; poll it as a fallback while the
      // stream is down, and while an operation looks active — the poll is
      // what reconciles a stale op if its completion frame never arrives.
      if (!eventsConnectedRef.current || operationActiveRef.current) {
        fetchProgress();
      }
    }, 3000);
    return () => clearInterval(interval);
  }, [fetchStatus, fetchProgress]);

  useEffect(() => {
    fetchProcesses();
    const interval = setInterval(fetchProcesses, 5000);
    return () => clearInterval(interval);
  }, [fetchProcesses]);

  useEffect(() => {
    if (logSource) {
      fetchLogs();
      const interval = setInterval(fetchLogs, 2000);
      return () => clearInterval(interval);
    }
  }, [logSource, fetchLogs]);

  // ── Node action handlers (cluster-scoped) ──

  const requestNodeAction = (cluster: string, type: 'stop-node' | 'restart-node' | 'start-node', nodeId: number) => {
    if (stackBusy) return;
    const disp = clusterDisplayOf(cluster);
    const copy: Record<typeof type, { title: string; message: string; confirmLabel: string; confirmStyle: 'danger' | 'warning' | 'primary' }> = {
      'stop-node': {
        title: `Stop Node ${nodeId}?`,
        message: `This will stop node ${nodeId} of the ${disp}. The cluster will continue with remaining nodes.`,
        confirmLabel: 'Stop Node',
        confirmStyle: 'danger',
      },
      'restart-node': {
        title: `Restart Node ${nodeId}?`,
        message: `This will restart node ${nodeId} of the ${disp}. It will temporarily leave the cluster and rejoin.`,
        confirmLabel: 'Restart Node',
        confirmStyle: 'warning',
      },
      'start-node': {
        title: `Start Node ${nodeId}?`,
        message: `This will start node ${nodeId} of the ${disp} and it will attempt to rejoin the cluster.`,
        confirmLabel: 'Start Node',
        confirmStyle: 'primary',
      },
    };
    setPendingAction({ type, cluster, nodeId, ...copy[type] });
  };

  const requestAllNodes = (cluster: string, type: 'stop-all-nodes' | 'start-all-nodes') => {
    if (stackBusy) return;
    const disp = clusterDisplayOf(cluster);
    if (type === 'stop-all-nodes') {
      setPendingAction({
        type, cluster,
        title: 'Stop All Nodes?',
        message: `This will stop all ${disp} nodes. The cluster will become completely unavailable.`,
        confirmLabel: 'Stop All',
        confirmStyle: 'danger',
      });
    } else {
      setPendingAction({
        type, cluster,
        title: 'Start All Nodes?',
        message: `This will start all ${disp} nodes and form a new cluster.`,
        confirmLabel: 'Start All',
        confirmStyle: 'primary',
      });
    }
  };

  const requestCleanup = (cluster: string) => {
    if (stackBusy) return;
    const disp = clusterDisplayOf(cluster);
    setPendingAction({
      type: 'cleanup', cluster,
      title: 'Clean Aeron State?',
      message: `This will remove stale Aeron files (shared memory, locks) for the ${disp}. All its nodes must be stopped first.`,
      confirmLabel: 'Clean State',
      confirmStyle: 'warning',
    });
  };

  const executeNodeAction = async (cluster: string, action: string, nodeId: number) => {
    setActiveOpCluster(cluster);
    try {
      const response = await fetch(adminUrl(`/api/admin/${action}`, { cluster }), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nodeId }),
      });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        toast({ tone: 'error', text: data.error || `Failed to ${action.replace('-', ' ')} (HTTP ${response.status})`, sticky: true });
      }
    } catch {
      toast({ tone: 'error', text: `Failed to ${action.replace('-', ' ')}`, sticky: true });
    }
  };

  const executeAllNodes = async (cluster: string, action: 'stop-all-nodes' | 'start-all-nodes') => {
    setActiveOpCluster(cluster);
    try {
      const response = await fetch(adminUrl(`/api/admin/${action}`, { cluster }), { method: 'POST' });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        toast({ tone: 'error', text: data.error || `Failed to ${action.replace(/-/g, ' ')} (HTTP ${response.status})`, sticky: true });
      }
    } catch {
      toast({ tone: 'error', text: `Failed to ${action.replace(/-/g, ' ')}`, sticky: true });
    }
  };

  const executeCleanup = async (cluster: string) => {
    setActiveOpCluster(cluster);
    try {
      const response = await fetch(adminUrl('/api/admin/cleanup', { cluster }), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ force: true }),
      });
      const data = await response.json();
      if (!data.success) {
        toast({ tone: 'error', text: data.error || 'Cleanup failed', sticky: true });
      }
    } catch {
      toast({ tone: 'error', text: 'Failed to cleanup state', sticky: true });
    }
  };

  // ── Generic process action handler (shared services) ──

  const requestProcessAction = (service: string, action: 'start' | 'stop' | 'restart') => {
    if (operatingServices.has(service) || stackBusy) return;

    const displayName = processes?.find(p => p.name === service)?.display || service;
    const actionLabel = action.charAt(0).toUpperCase() + action.slice(1);

    const descriptions: Record<string, Record<string, string>> = {
      stop: {
        backup: 'This will stop the backup node. Cluster snapshots will not be available until restarted.',
        market: 'This will stop the market data WebSocket. Clients will lose real-time market updates.',
        order: 'This will stop the order API. Order submission will be unavailable.',
        admin: 'This will stop the admin gateway. You will lose access to this dashboard.',
        ui: 'This will stop the trading UI. Users will not be able to access the web interface.',
      },
      start: {
        backup: 'This will start the backup node to enable cluster state backups.',
        market: 'This will start the market data WebSocket for real-time updates.',
        order: 'This will start the order API for order submission.',
        admin: 'This will start the admin gateway.',
        ui: 'This will start the trading UI web interface.',
      },
      restart: {
        backup: 'This will restart the backup node. Backup service will be temporarily unavailable.',
        market: 'This will restart the market gateway. Clients will be temporarily disconnected.',
        order: 'This will restart the order gateway. Order submission will be temporarily unavailable.',
        admin: 'This will restart the admin gateway. You will temporarily lose access to this dashboard.',
        ui: 'This will restart the trading UI. Users will experience a brief interruption.',
      },
    };

    const styles: Record<string, 'danger' | 'warning' | 'primary'> = {
      stop: 'danger', start: 'primary', restart: 'warning',
    };

    setPendingAction({
      type: 'process-action',
      service,
      action,
      title: `${actionLabel} ${displayName}?`,
      message: descriptions[action]?.[service] || `This will ${action} the ${displayName} service.`,
      confirmLabel: actionLabel,
      confirmStyle: styles[action],
    });
  };

  const executeProcessAction = async (service: string, action: string) => {
    setOperatingServices(prev => new Set(prev).add(service));
    const clearOperating = () => setOperatingServices(prev => {
      const next = new Set(prev);
      next.delete(service);
      return next;
    });
    try {
      const response = await fetch(adminUrl(`/api/admin/processes/${service}/${action}`), { method: 'POST' });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        toast({ tone: 'error', text: data.error || `Failed to ${action} ${service} (HTTP ${response.status})`, sticky: true });
        clearOperating();
        return;
      }
      const timeout = action === 'restart' ? 8000 : 3000;
      setTimeout(() => {
        clearOperating();
        fetchProcesses();
      }, timeout);
    } catch {
      toast({ tone: 'error', text: `Failed to ${action} ${service}`, sticky: true });
      clearOperating();
    }
  };

  // ── Snapshot (per cluster, from the rail) ──

  const requestSnapshot = (cluster: string) => {
    if (stackBusy || snapshotOps.has(cluster)) return;
    const disp = clusterDisplayOf(cluster);
    setPendingAction({
      type: 'snapshot', cluster,
      title: `Take a snapshot of the ${disp}?`,
      message: `Captures a consistent snapshot of the ${disp} cluster state for fast recovery. Safe to run on the live cluster.`,
      confirmLabel: 'Take Snapshot',
      confirmStyle: 'primary',
    });
  };

  const executeSnapshot = async (cluster: string) => {
    setSnapshotOps(prev => new Set(prev).add(cluster));
    setActiveOpCluster(cluster);
    const clear = () => setSnapshotOps(prev => {
      const next = new Set(prev);
      next.delete(cluster);
      return next;
    });
    try {
      const response = await fetch(adminUrl('/api/admin/snapshot', { cluster }), { method: 'POST' });
      if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        toast({ tone: 'error', text: data.error || `Failed to take snapshot (HTTP ${response.status})`, sticky: true });
        clear();
        return;
      }
      setTimeout(clear, 5000);
    } catch {
      toast({ tone: 'error', text: 'Failed to take snapshot', sticky: true });
      clear();
    }
  };

  // ── Rolling operations (per cluster) ──

  const requestHousekeeping = (cluster: string) => {
    if (stackBusy) return;
    const disp = clusterDisplayOf(cluster);
    setPendingAction({
      type: 'housekeeping', cluster,
      title: 'Start Archive Housekeeping?',
      message: `Reclaims archive disk on the live ${disp} by purging log segments below the latest snapshot. Live-safe; refused if any node is down or lagging.`,
      confirmLabel: 'Start Housekeeping',
      confirmStyle: 'warning',
    });
  };

  const executeHousekeeping = async (cluster: string, force: boolean) => {
    setActiveOpCluster(cluster);
    try {
      const response = await fetch(adminUrl('/api/admin/housekeeping', { cluster }), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(force ? { force: true } : {}),
      });
      if (!response.ok) {
        const data = await response.json();
        if (response.status === 409 && !force && data.error) {
          // The lag guard refused (a node is down/lagging — purging would
          // strand it). Offer an explicit, clearly-dangerous override.
          setPendingAction({
            type: 'housekeeping-force', cluster,
            title: 'Housekeeping Refused — Force?',
            message: `The server refused: ${data.error}. Forcing while a member is down or lagging can strand it permanently. Only continue if you know why.`,
            confirmLabel: 'Force Housekeeping',
            confirmStyle: 'danger',
          });
          return;
        }
        toast({ tone: 'error', text: data.error || 'Housekeeping failed', sticky: true });
      }
    } catch {
      toast({ tone: 'error', text: 'Failed to trigger housekeeping', sticky: true });
    }
  };

  // ── Confirm action dispatch ──

  const confirmAction = async () => {
    if (!pendingAction) return;
    const action = pendingAction;
    setPendingAction(null);

    switch (action.type) {
      case 'stop-node':
      case 'restart-node':
      case 'start-node':
        if (action.nodeId !== undefined && action.cluster) {
          await executeNodeAction(action.cluster, action.type, action.nodeId);
        }
        break;
      case 'process-action':
        if (action.service && action.action) {
          await executeProcessAction(action.service, action.action);
        }
        break;
      case 'housekeeping':
        if (action.cluster) await executeHousekeeping(action.cluster, false);
        break;
      case 'housekeeping-force':
        if (action.cluster) await executeHousekeeping(action.cluster, true);
        break;
      case 'snapshot':
        if (action.cluster) await executeSnapshot(action.cluster);
        break;
      case 'stop-all-nodes':
        if (action.cluster) await executeAllNodes(action.cluster, 'stop-all-nodes');
        break;
      case 'start-all-nodes':
        if (action.cluster) await executeAllNodes(action.cluster, 'start-all-nodes');
        break;
      case 'cleanup':
        if (action.cluster) await executeCleanup(action.cluster);
        break;
    }
  };

  // ── Derived state ──

  // Every process that is a cluster node (node0-2, ae0, …) is filtered out of
  // the Services tab, whatever its backend role.
  const clusterNodeNames = new Set<string>(
    clusters?.flatMap(c => c.nodes.map(n => n.procName ?? (c.kind === 'match' ? `node${n.id}` : `${c.name}${n.id}`))) ?? [],
  );

  const tabClass = (active: boolean) =>
    `relative -mb-px border-b-2 px-4 py-2.5 text-[13px] font-medium font-display transition-colors ${
      active
        ? 'border-accent text-accent'
        : 'border-transparent text-muted hover:text-text'
    }`;

  return (
    <div className="min-h-screen bg-bg text-text">
      {/* Top bar */}
      <header className="sticky top-0 z-20 flex items-center gap-4 border-b border-hairline bg-surface/95 px-6 py-3 backdrop-blur">
        <a
          href={TRADING_URL}
          className="flex items-center gap-1.5 rounded-md px-2 py-1 text-[13px] font-medium text-muted transition-colors hover:bg-surface-2 hover:text-text [&_svg]:h-4 [&_svg]:w-4"
        >
          {Icons.back}
          <span>Trading</span>
        </a>
        <div className="h-5 w-px bg-hairline" />
        <div className="flex select-none items-center gap-2.5">
          <LogoMark className="h-[22px] w-[22px]" />
          <h1 className="font-display text-[17px] font-semibold leading-none tracking-tight text-text-strong">
            <span className="text-accent">Open</span> Exchange — Admin
          </h1>
        </div>
        <div className="ml-auto flex items-center gap-3">
          <a
            href={GRAFANA_URL}
            target="_blank"
            rel="noreferrer noopener"
            className="flex items-center gap-1.5 rounded-md px-2 py-1 text-[13px] font-medium text-muted transition-colors hover:bg-surface-2 hover:text-text [&_svg]:h-3.5 [&_svg]:w-3.5"
          >
            <span>Grafana</span>
            {Icons.external}
          </a>
          <GatewayIndicator gatewayOk={gatewayOk} eventsConnected={eventsConnected} />
          <ThemeToggle theme={theme} onToggle={toggle} />
        </div>
      </header>

      {/* Tab bar */}
      <div className="border-b border-hairline bg-surface px-6">
        <nav className="mx-auto flex max-w-[1280px] gap-1">
          <button className={tabClass(tab === 'overview')} onClick={() => setTab('overview')}>Overview</button>
          <button className={tabClass(tab === 'clusters')} onClick={() => setTab('clusters')}>Clusters</button>
          <button className={tabClass(tab === 'services')} onClick={() => setTab('services')}>Services</button>
          <button className={tabClass(tab === 'risk')} onClick={() => setTab('risk')}>Risk</button>
          <button className={tabClass(tab === 'backup')} onClick={() => setTab('backup')}>Backup</button>
        </nav>
      </div>

      <div className="mx-auto max-w-[1280px] px-6 pb-12 pt-6">
        {tab === 'overview' && (
          <OverviewDashboard
            clusters={clusters}
            status={status}
            processSummary={processSummary}
            onOpenClusters={() => setTab('clusters')}
          />
        )}

        {tab === 'services' && (
          <ServicesSection
            processes={processes}
            hidden={clusterNodeNames}
            operatingServices={operatingServices}
            stackBusy={stackBusy}
            logSource={logSource}
            onProcessAction={requestProcessAction}
            onViewLogs={setLogSource}
          />
        )}

        {tab === 'risk' && <RiskAdmin />}
        {tab === 'backup' && <BackupOps clusters={clusters ?? undefined} />}

        {tab === 'clusters' && (
          <main className="flex flex-col gap-8">
            {clusters === null ? (
              <ClusterSkeleton />
            ) : (
              clusters.map((c) => {
                // The shared progress record belongs to exactly one cluster;
                // default unattributed/auto ops to 'match'. Only the targeted
                // cluster shows the % hero + swapped slot; others stay locked.
                const operation = stackBusy && (activeOpCluster ?? 'match') === c.name ? progress : null;
                return (
                  <ClusterSection
                    key={c.name}
                    cluster={c}
                    processes={processes ?? []}
                    operation={operation}
                    stackBusy={stackBusy}
                    snapshotBusy={snapshotOps.has(c.name)}
                    logSource={logSource}
                    onNodeAction={requestNodeAction}
                    onAllNodes={requestAllNodes}
                    onCleanup={requestCleanup}
                                    onHousekeeping={requestHousekeeping}
                    onSnapshot={requestSnapshot}
                    onViewLogs={setLogSource}
                  />
                );
              })
            )}

            {/* Live activity feed (SSE) */}
            <EventFeed
              entries={feedEntries}
              connected={eventsConnected}
              open={feedOpen}
              unseen={feedUnseen}
              onToggle={() => {
                setFeedOpen((o) => {
                  if (!o) markFeedSeen();
                  return !o;
                });
              }}
            />

            <LogViewer
              logSource={logSource}
              logs={logs}
              unavailable={logsUnavailable}
              resolveClusterDisplay={clusterDisplayOf}
              onClear={() => setLogSource(null)}
            />
          </main>
        )}
      </div>

      {/* Confirmation Modal (cluster + service actions) */}
      {pendingAction && (
        <ConfirmModal
          title={pendingAction.title}
          body={pendingAction.message}
          tone={pendingAction.confirmStyle}
          confirmLabel={pendingAction.confirmLabel}
          requireText={pendingAction.requireText}
          onConfirm={confirmAction}
          onCancel={() => setPendingAction(null)}
        />
      )}
    </div>
  );
}
