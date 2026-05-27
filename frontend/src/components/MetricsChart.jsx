import { useMemo } from "react";
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
import { formatBucketTime, parseUtc } from "../utils";

// A line chart of the per-bucket counts over the selected range,
// rendered with Recharts. Three lines: keystrokes, words,
// corrections. Clicks are excluded so the typing signal is not
// drowned out by mouse activity.
//
// Buckets are produced by the parent ActivityPanel and shared with
// the IdleStrip above, so cell N of the strip lines up with the
// chart's Nth x-axis tick. connectNulls={false} keeps any synthetic
// (idle) rows from being smoothed across.
//
// The card chrome (border, title, padding) is provided by the
// parent ActivityPanel so the chart can sit directly beneath the
// IdleStrip with no nested borders.
export function MetricsChart({ buckets, totalMinutes }) {
  // useMemo so the array isn't rebuilt on unrelated parent renders.
  // At 1w there are up to 168 buckets — the savings are small but
  // real and the dependency list is honest.
  const data = useMemo(
    () =>
      buckets.map((m) => ({
        time: formatBucketTime(parseUtc(m.window_start), totalMinutes),
        keystrokes: m.keystrokes,
        words: m.words,
        corrections: m.corrections,
      })),
    [buckets, totalMinutes],
  );

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
