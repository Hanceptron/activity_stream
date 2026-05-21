import { useState } from "react";
import { Header } from "./components/Header";
import { MetricCards } from "./components/MetricCards";
import { MetricsChart } from "./components/MetricsChart";
import { Heatmap } from "./components/Heatmap";
import { RangeSelector } from "./components/RangeSelector";
import { SessionsList } from "./components/SessionsList";
import { usePolling } from "./usePolling";

// Top-level wiring. Polling intervals match how often each
// upstream output changes:
// - metrics: 5 s (streaming job emits a new window every minute).
// - sessions, baseline, heatmap: 30 s. These three come from the
//   batch job and only change when the batch job runs, so polling
//   any faster would just resend the same payload.
export default function App() {
  const [range, setRange] = useState("1h");

  const metrics = usePolling("/api/metrics", 5_000);
  const sessions = usePolling("/api/sessions", 30_000);
  const heatmap = usePolling(`/api/heatmap?range=${range}`, 30_000);
  const baseline = usePolling("/api/baseline", 5 * 60_000);

  return (
    <div className="min-h-screen bg-zinc-900 text-zinc-100">
      <div className="max-w-7xl mx-auto p-6 space-y-6">
        <Header metrics={metrics} />
        <MetricCards metrics={metrics} baseline={baseline} />
        <MetricsChart metrics={metrics} />
        <section className="space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-sm text-zinc-400">Spatial heatmaps</h2>
            <RangeSelector value={range} onChange={setRange} />
          </div>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Heatmap data={heatmap} type="move" title="Movement" color="#3b82f6" />
            <Heatmap data={heatmap} type="click" title="Clicks" color="#ef4444" />
          </div>
        </section>
        <SessionsList sessions={sessions} />
      </div>
    </div>
  );
}
