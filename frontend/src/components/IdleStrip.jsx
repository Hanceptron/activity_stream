import { parseUtc, zeroFillWindows } from "../utils";

// Horizontal strip of 60 cells, one per minute over the last hour.
// Green when any activity happened in that minute, dark gray when
// the user was idle. Hovering a cell reveals the time + counts via
// the native title attribute.
//
// flex-1 on each cell makes the strip exactly fill its container,
// which is sized to match the chart below it in ActivityPanel —
// so cell N visually corresponds to the same minute as the chart's
// Nth x-axis tick.
export function IdleStrip({ metrics }) {
  const filled = zeroFillWindows(metrics, 60);

  return (
    <div
      className="flex gap-px"
      role="img"
      aria-label="Per-minute activity timeline for the last 60 minutes"
    >
      {filled.map((m, i) => {
        const totalActivity =
          (m.keystrokes ?? 0) +
          (m.words ?? 0) +
          (m.corrections ?? 0) +
          (m.clicks ?? 0);
        const active = totalActivity > 0;
        const time = parseUtc(m.window_start);
        const timeStr = time
          ? time.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
          : "";
        const counts = active
          ? `${m.keystrokes ?? 0} keys, ${m.clicks ?? 0} clicks`
          : "idle";

        return (
          <div
            key={i}
            className={`flex-1 h-3 rounded-sm ${
              active ? "bg-green-500" : "bg-zinc-700"
            }`}
            title={`${timeStr} — ${counts}`}
          />
        );
      })}
    </div>
  );
}
