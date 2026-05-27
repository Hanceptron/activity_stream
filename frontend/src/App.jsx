import { useMemo, useState } from "react";
import { Header } from "./components/Header";
import { TodayTotals } from "./components/TodayTotals";
import { ActivityPanel } from "./components/ActivityPanel";
import { MetricCards } from "./components/MetricCards";
import { Heatmap } from "./components/Heatmap";
import { HotspotsLeaderboard } from "./components/HotspotsLeaderboard";
import { InputMixIndicator } from "./components/InputMixIndicator";
import { RangeSelector } from "./components/RangeSelector";
import { ActivityGauge } from "./components/ActivityGauge";
import { SessionsList } from "./components/SessionsList";
import { StalenessChip } from "./components/StalenessChip";
import { usePolling } from "./usePolling";
import { ACTIVITY_RANGES, filterByUser, parseUtc } from "./utils";

// Top-level wiring. Polling intervals match how often each
// upstream output changes:
// - metrics: 5 s (streaming job emits a new window every minute).
// - sessions, baseline, heatmap, batch_status: 30 s. These four
//   come from the batch job (or its scheduler state) and only
//   change when the batch job runs, so polling any faster would
//   just resend the same payload. The scheduler runs every 5 min
//   inside the API process; see streamguard/api.py.
//
// Multi-user filtering: every endpoint includes a `user` field.
// The dashboard scopes every panel to a single user. `selectedUser`
// is the explicit user choice from the dropdown; `effectiveUser` is
// the user actually rendered — either the explicit choice (if it
// is still present in the metrics stream) or the user with the
// newest metric window otherwise. Deriving `effectiveUser` during
// render avoids a setState-in-effect cascade.
export default function App() {
  const [range, setRange] = useState("1h");
  const [activityRange, setActivityRange] = useState("1h");
  const [selectedUser, setSelectedUser] = useState(null);

  const activityCfg = ACTIVITY_RANGES[activityRange];
  const activityMinutes = activityCfg.bucketCount * activityCfg.bucketSizeMin;

  // The 1h preset's poll interval is also the right cadence for the
  // header/metric-card widgets that only ever read the newest minute.
  const metrics = usePolling("/api/metrics", ACTIVITY_RANGES["1h"].pollMs);
  const activityMetrics = usePolling(
    `/api/metrics?minutes=${activityMinutes}`,
    activityCfg.pollMs,
  );
  const sessions = usePolling("/api/sessions", 30_000);
  const heatmap = usePolling(`/api/heatmap?range=${range}`, 30_000);
  const baseline = usePolling("/api/baseline", 5 * 60_000);
  const batchStatus = usePolling("/api/batch_status", 30_000);

  const lastBatchRun = batchStatus?.last_run ?? null;
  const batchStatusName = batchStatus?.status ?? "idle";

  // Derived user list from the metrics stream. null while metrics
  // is null so UserSelector can stay hidden until first fetch.
  const users = useMemo(() => {
    if (!metrics) return null;
    return Array.from(new Set(metrics.map((m) => m.user).filter(Boolean))).sort();
  }, [metrics]);

  // Pure derivation: honor the dropdown choice when it's still
  // valid, otherwise fall back to the newest-activity user.
  const effectiveUser = useMemo(() => {
    if (selectedUser && users && users.includes(selectedUser)) {
      return selectedUser;
    }
    if (!metrics || metrics.length === 0) return null;

    let newestUser = null;
    let newestTime = -Infinity;
    for (const m of metrics) {
      const t = parseUtc(m.window_start)?.getTime();
      if (t != null && t > newestTime) {
        newestTime = t;
        newestUser = m.user;
      }
    }
    return newestUser;
  }, [metrics, users, selectedUser]);

  const metricsForUser = filterByUser(metrics, effectiveUser);
  const activityMetricsForUser = filterByUser(activityMetrics, effectiveUser);
  const sessionsForUser = filterByUser(sessions, effectiveUser);
  const heatmapForUser = filterByUser(heatmap, effectiveUser);
  const baselineForUser =
    baseline && effectiveUser
      ? baseline.find((b) => b.user === effectiveUser) ?? null
      : null;

  return (
    <div className="min-h-screen bg-zinc-900 text-zinc-100">
      <div className="max-w-7xl mx-auto p-6 space-y-6">
        <Header
          metrics={metricsForUser}
          sessions={sessionsForUser}
          users={users}
          selectedUser={effectiveUser}
          onSelectUser={setSelectedUser}
        />
        <TodayTotals sessions={sessionsForUser} />
        <MetricCards metrics={metricsForUser} baseline={baselineForUser} />
        <section className="space-y-4">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <h2 className="text-sm text-zinc-400">Activity</h2>
            <RangeSelector value={activityRange} onChange={setActivityRange} />
          </div>
          <ActivityGauge metrics={activityMetricsForUser} range={activityRange} />
          <ActivityPanel metrics={activityMetricsForUser} range={activityRange} />
        </section>
        <InputMixIndicator
          latest={
            metricsForUser && metricsForUser.length > 0
              ? metricsForUser[metricsForUser.length - 1]
              : null
          }
        />
        <section className="space-y-3">
          <div className="flex items-center justify-between gap-3 flex-wrap">
            <div className="flex items-baseline gap-3">
              <h2 className="text-sm text-zinc-400">Spatial heatmaps</h2>
              <StalenessChip lastRunIso={lastBatchRun} status={batchStatusName} />
            </div>
            <RangeSelector value={range} onChange={setRange} />
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Heatmap data={heatmapForUser} type="move" title="Movement" color="#3b82f6" />
            <Heatmap data={heatmapForUser} type="click" title="Clicks" color="#ef4444" />
          </div>
          <HotspotsLeaderboard
            heatmap={heatmapForUser}
            lastRunIso={lastBatchRun}
            status={batchStatusName}
          />
        </section>
        <SessionsList
          sessions={sessionsForUser}
          lastRunIso={lastBatchRun}
          status={batchStatusName}
        />
      </div>
    </div>
  );
}
