import {
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

// Tiny line chart of flight_time_std across the last 60 min, with
// a dashed reference line at the user's baseline mean (when known).
//
// Filters to rows where flight_time_std is non-null rather than
// using zeroFillWindows: a window with no keystrokes has no
// meaningful rhythm value, and synthesizing zero would mislead the
// eye into seeing "improving rhythm" during idle gaps.
//
// Indexed by reliable-row ordinal, not actual time, because the
// filtering would otherwise produce unequal x-spacing. The
// surrounding panel framing (sits inside RhythmPanel, next to the
// "Last 60 minutes" chart) gives the user the temporal anchor.
//
// Hidden when fewer than 2 reliable points exist — a single-point
// sparkline conveys nothing and a missing component is less jarring
// than an empty chart frame.
export function RhythmSparkline({ metrics, baselineMean }) {
  const points = (metrics || [])
    .filter((m) => m.flight_time_std != null)
    .map((m, i) => ({ i, value: m.flight_time_std }));

  if (points.length < 2) return null;

  return (
    <div
      role="img"
      aria-label="Flight-time std trend over the last hour, compared to baseline"
    >
      <ResponsiveContainer width="100%" height={40}>
        <LineChart
          data={points}
          margin={{ top: 2, right: 2, bottom: 2, left: 2 }}
        >
          {baselineMean != null && !Number.isNaN(baselineMean) && (
            <ReferenceLine
              y={baselineMean}
              stroke="#52525b"
              strokeDasharray="2 2"
            />
          )}
          <Line
            type="monotone"
            dataKey="value"
            stroke="#a1a1aa"
            dot={false}
            strokeWidth={1.5}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
