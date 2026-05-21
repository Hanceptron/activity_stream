import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { parseUtc } from "../utils";

// A line chart of the per-minute counts over the last 60 minutes,
// rendered with Recharts. Three lines: keystrokes, words,
// corrections. Clicks are excluded so the typing signal is not
// drowned out by mouse activity.
export function MetricsChart({ metrics }) {
  const data = (metrics || []).map((m) => ({
    time: parseUtc(m.window_start).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
    keystrokes: m.keystrokes,
    words: m.words,
    corrections: m.corrections,
  }));

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <h2 className="text-sm text-zinc-400 mb-3">Last 60 minutes</h2>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#3f3f46" />
          <XAxis dataKey="time" stroke="#a1a1aa" fontSize={12} />
          <YAxis stroke="#a1a1aa" fontSize={12} />
          <Tooltip
            contentStyle={{
              background: "#27272a",
              border: "1px solid #3f3f46",
              borderRadius: 6,
              color: "#e4e4e7",
            }}
          />
          <Legend wrapperStyle={{ color: "#d4d4d8" }} />
          <Line type="monotone" dataKey="keystrokes" stroke="#3b82f6" dot={false} strokeWidth={2} />
          <Line type="monotone" dataKey="words" stroke="#10b981" dot={false} strokeWidth={2} />
          <Line type="monotone" dataKey="corrections" stroke="#ef4444" dot={false} strokeWidth={2} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
