// SPDX-License-Identifier: Apache-2.0
import type { AdminEventType, ClusterEventType, FeedEntry } from '../../hooks/useAdminEvents';

interface EventFeedProps {
  entries: FeedEntry[];
  connected: boolean;
  open: boolean;
  onToggle: () => void;
  unseen: number;
}

// Semantic pill styles per event type, reusing the console's existing
// badge palette (NODE_ROLE_BADGE idiom): green = came up, red = went down
// badly, amber = collateral, accent = adopted after an admin restart.
const EVENT_BADGE: Record<AdminEventType, string> = {
  started: 'bg-buy-soft text-buy',
  stopped: 'bg-surface-2 text-muted',
  crashed: 'bg-sell-soft text-sell',
  'cascade-stop': 'bg-warn-soft text-warn',
  disarmed: 'bg-sell-soft text-sell',
  adopted: 'bg-accent-soft text-accent',
};

const EVENT_DOT: Record<AdminEventType, string> = {
  started: 'bg-buy',
  stopped: 'bg-faint',
  crashed: 'bg-sell',
  'cascade-stop': 'bg-warn',
  disarmed: 'bg-sell',
  adopted: 'bg-accent',
};

// Cluster transitions, on the same palette. QUORUM_LOST is the one that means
// the engine has stopped accepting writes, so it is the only red that is not
// about a single node.
const CLUSTER_BADGE: Record<ClusterEventType, string> = {
  LEADER_CHANGE: 'bg-accent-soft text-accent',
  NODE_UP: 'bg-buy-soft text-buy',
  NODE_DOWN: 'bg-sell-soft text-sell',
  QUORUM_LOST: 'bg-sell-soft text-sell',
  QUORUM_RESTORED: 'bg-buy-soft text-buy',
};

const CLUSTER_DOT: Record<ClusterEventType, string> = {
  LEADER_CHANGE: 'bg-accent',
  NODE_UP: 'bg-buy',
  NODE_DOWN: 'bg-sell',
  QUORUM_LOST: 'bg-sell',
  QUORUM_RESTORED: 'bg-buy',
};

function timeOf(at: string): string {
  const d = new Date(at);
  return isNaN(d.getTime()) ? '' : d.toTimeString().slice(0, 8);
}

/**
 * Live activity feed over /api/admin/events, carrying two streams in ONE
 * chronological list: agent lifecycle (crashes, cascades, disarms, starts,
 * stops, adoptions) and cluster transitions (elections, node losses, quorum).
 *
 * They are merged rather than split because the diagnostic sequence is "node
 * crashed, then the leader moved, then quorum went" — two panels make the
 * operator reconstruct that by hand. The cluster half used to live in the
 * trading UI, fed by the market socket, which only ever carried the matching
 * engine; here it covers the money ledger too.
 *
 * Collapsible so the Cluster tab stays calm; the unseen badge keeps crashes
 * from hiding. Rows are keyed by a monotonic seq and never animate on mount
 * (stable keys, no flicker — the trade-tape lesson).
 */
export function EventFeed({ entries, connected, open, onToggle, unseen }: EventFeedProps) {
  return (
    <section className="rounded-lg border border-hairline bg-surface p-6">
      <button
        className="flex w-full items-center gap-2.5 text-left"
        onClick={onToggle}
        aria-expanded={open}
      >
        <span
          className={`h-2 w-2 rounded-full ${connected ? 'bg-buy' : 'bg-faint'}`}
          title={connected ? 'Live event stream connected' : 'Event stream disconnected'}
        />
        <h2 className="flex-1 text-[11px] font-semibold uppercase tracking-wider text-muted">
          Activity
        </h2>
        {!open && unseen > 0 && (
          <span className="rounded-full bg-accent-soft px-2 py-0.5 font-mono text-[10px] font-semibold tabular-nums text-accent">
            {unseen > 99 ? '99+' : unseen}
          </span>
        )}
        <span className="text-[11px] text-faint">{open ? 'Hide' : 'Show'}</span>
      </button>

      {open && (
        <div className="mt-4 max-h-64 overflow-y-auto rounded-md border border-hairline bg-surface-2">
          {entries.length === 0 ? (
            <div className="px-4 py-6 text-center text-[12px] italic text-faint">
              No events yet — service starts, stops and crashes, plus cluster elections,
              node losses and quorum changes, appear here live.
            </div>
          ) : (
            <ul>
              {entries.map((entry) => {
                // One chronological story across both streams: the row shape is
                // identical (time, dot, subject, badge, detail); only the
                // subject differs — a service name, or the cluster it happened to.
                const { seq } = entry;
                const isCluster = entry.cluster !== undefined;
                const at = isCluster ? entry.cluster!.at : entry.ev!.at;
                const type = isCluster ? entry.cluster!.type : entry.ev!.type;
                const dot = isCluster
                  ? CLUSTER_DOT[entry.cluster!.type]
                  : EVENT_DOT[entry.ev!.type];
                const badge = isCluster
                  ? CLUSTER_BADGE[entry.cluster!.type]
                  : EVENT_BADGE[entry.ev!.type];
                const subject = isCluster
                  ? entry.cluster!.nodeId >= 0
                    ? `${entry.cluster!.display} · node ${entry.cluster!.nodeId}`
                    : entry.cluster!.display
                  : entry.ev!.service;
                const detail = isCluster ? entry.cluster!.detail : entry.ev!.detail;

                return (
                  <li
                    key={seq}
                    className="flex items-baseline gap-2.5 border-b border-hairline px-4 py-1.5 last:border-b-0"
                  >
                    <span className="shrink-0 font-mono text-[11px] tabular-nums text-faint">
                      {timeOf(at)}
                    </span>
                    <span className={`mb-0.5 h-1.5 w-1.5 shrink-0 self-center rounded-full ${dot || 'bg-faint'}`} />
                    <span className="shrink-0 font-mono text-[11px] font-semibold text-text">
                      {subject}
                    </span>
                    <span className={`shrink-0 rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${badge || 'bg-surface-2 text-muted'}`}>
                      {type}
                    </span>
                    {!isCluster && entry.ev!.pid ? (
                      <span className="shrink-0 font-mono text-[10px] tabular-nums text-faint">
                        pid {entry.ev!.pid}
                      </span>
                    ) : null}
                    {detail ? (
                      <span className="truncate text-[11px] text-muted" title={detail}>
                        {detail}
                      </span>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </section>
  );
}
