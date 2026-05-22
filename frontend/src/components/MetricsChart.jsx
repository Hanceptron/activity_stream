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
import { parseUtc, zeroFillWindows } from "../utils";

// A line chart of the per-minute counts over the last 60 minutes,
// rendered with Recharts. Three lines: keystrokes, words,
// corrections. Clicks are excluded so the typing signal is not
// drowned out by mouse activity.
//
// zeroFillWindows ensures every minute in the trailing 60-min
// window has a row, so idle stretches render as flat zero rather
// than as a smooth connecting line. connectNulls={false} is set
// defensively in case any synthetic row is ever generated with a
// null value.
//
// The card chrome (border, title, padding) is provided by the
// parent ActivityPanel so the chart can sit directly beneath the
// IdleStrip with no nested borders.
export function MetricsChart({ metrics }) {
  const data = zeroFillWindows(metrics).map((m) => ({
    time: parseUtc(m.window_start).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
    keystrokes: m.keystrokes,
    words: m.words,
    corrections: m.corrections,
  }));

  return (
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
        <Line type="monotone" dataKey="keystrokes" stroke="#3b82f6" dot={false} strokeWidth={2} connectNulls={false} />
        <Line type="monotone" dataKey="words" stroke="#10b981" dot={false} strokeWidth={2} connectNulls={false} />
        <Line type="monotone" dataKey="corrections" stroke="#ef4444" dot={false} strokeWidth={2} connectNulls={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
