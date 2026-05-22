import { IdleStrip } from "./IdleStrip";
import { MetricsChart } from "./MetricsChart";

// One card combining the idle/active strip with the per-minute line
// chart. Both views share the same 60-minute trailing window so the
// strip's cells line up with the chart's x-axis ticks below.
//
// The chrome (card border + title) lives here rather than in
// MetricsChart so the strip and the chart visually belong together.
export function ActivityPanel({ metrics }) {
  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <h2 className="text-sm text-zinc-400 mb-3">Last 60 minutes</h2>
      <div className="mb-3">
        <IdleStrip metrics={metrics} />
      </div>
      <MetricsChart metrics={metrics} />
    </div>
  );
}
