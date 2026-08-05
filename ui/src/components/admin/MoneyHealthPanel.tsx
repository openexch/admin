// SPDX-License-Identifier: Apache-2.0
// Assets-engine ledger-integrity readout. When `money` is present it reads
// conservation, the last applied trade id, settlement lag and the check time;
// a broken conservation check is sell-toned here AND escalates the rail hero
// (see getClusterStatus).
//
// RENDERS NOTHING when `money` is absent, which today is always: no admin-gateway
// code path emits a `money` key (buildRichClusterStatus in services/status.go
// never sets one), so nothing has ever filled this panel. It used to print
// "not reported by this build" instead, and that was worse than blank: a section
// headed "Ledger Integrity" sitting on the dashboard reads as coverage to an
// operator scanning it, which is exactly the mistake the settlement canary made
// on 2026-07-25 while conservation went unchecked for seventeen hours. An
// unbuilt check must not occupy space that implies it is watching.
//
// The conservation math already exists offline in tools/money-check/money_check.py
// and aws-bench/reconcile.py. Wiring one of them to a live status field is what
// turns this panel on; until then it stays invisible.
import { Icons } from '../Icons';
import type { MoneyHealth } from './types';

interface MoneyHealthPanelProps {
  money?: MoneyHealth;
}

function Tile({ label, value, tone }: { label: string; value: string; tone?: 'sell' }) {
  return (
    <div className="flex flex-shrink-0 flex-col gap-0.5">
      <span className="text-[10px] font-medium uppercase tracking-wide text-faint">{label}</span>
      <span className={`font-mono text-[13px] tabular-nums ${tone === 'sell' ? 'text-sell' : 'text-text'}`}>{value}</span>
    </div>
  );
}

function formatCheckedAt(iso: string): string {
  const d = new Date(iso);
  return isNaN(d.getTime()) ? iso : d.toTimeString().slice(0, 8);
}

export function MoneyHealthPanel({ money }: MoneyHealthPanelProps) {
  if (money === undefined) return null;
  return (
    <section className="rounded-lg border border-hairline bg-surface p-4">
      <div className="mb-3 flex items-center gap-2.5 [&>svg]:h-4 [&>svg]:w-4 [&>svg]:text-faint">
        {Icons.database}
        <h2 className="text-[11px] font-semibold uppercase tracking-wider text-muted">Ledger Integrity</h2>
      </div>
      <div className="flex flex-wrap items-center gap-x-8 gap-y-3">
        <div className="flex flex-shrink-0 flex-col gap-0.5">
          <span className="text-[10px] font-medium uppercase tracking-wide text-faint">Conservation</span>
          <span className={`inline-flex items-center gap-1.5 font-mono text-[13px] font-semibold tabular-nums ${money.conservationOk ? 'text-buy' : 'text-sell'}`}>
            <span className={`h-1.5 w-1.5 rounded-full ${money.conservationOk ? 'bg-buy' : 'bg-sell animate-pulse-soft'}`} />
            {money.conservationOk ? 'OK' : 'BROKEN'}
          </span>
        </div>
        <Tile label="Last applied trade" value={money.lastAppliedTradeId.toLocaleString()} />
        {money.imbalanceMinor !== undefined && (
          <Tile label="Imbalance" value={money.imbalanceMinor.toLocaleString()} tone={money.imbalanceMinor !== 0 ? 'sell' : undefined} />
        )}
        {money.settlementLagMs !== undefined && (
          <Tile label="Settlement lag" value={`${money.settlementLagMs.toLocaleString()} ms`} />
        )}
        {money.checkedAt && <Tile label="Checked at" value={formatCheckedAt(money.checkedAt)} />}
      </div>
    </section>
  );
}
