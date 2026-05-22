import {
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
} from "recharts";

// Tiny line chart of fatigue_index across recent reliable sessions
// (oldest left, newest right). Sized to fit alongside the
// SessionsList header title.
//
// The x-axis is "session ordinal among reliable sessions," not
// actual time — sessions arrive at irregular intervals and would
// otherwise distort the trend. The y=0 reference line implicitly
// splits the area into "improving" (below) and "degrading" (above).
//
// Hidden entirely when fewer than 2 reliable points exist, since a
// one-point sparkline conveys nothing and a missing component is
// less jarring than an empty chart frame.
export function SessionFatigueSparkline({ sessions }) {
  const reliable = (sessions || []).filter((s) => s.fatigue_reliable);
  // /api/sessions already sorts newest-first; reverse for left=old.
  const points = reliable
    .slice(0, 20)
    .reverse()
    .map((s, i) => ({ i, fatigue: s.fatigue_index }));

  if (points.length < 2) return null;

  return (
    <div
      className="text-zinc-500"
      role="img"
      aria-label="Sparkline of fatigue index over recent reliable sessions"
    >
      <ResponsiveContainer width={120} height={28}>
        <LineChart
          data={points}
          margin={{ top: 2, right: 2, bottom: 2, left: 2 }}
        >
          <ReferenceLine y={0} stroke="#52525b" strokeDasharray="2 2" />
          <Line
            type="monotone"
            dataKey="fatigue"
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
