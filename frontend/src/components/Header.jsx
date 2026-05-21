import { parseUtc } from "../utils";

// Title plus a live indicator. The dot is green when the newest
// metric window is less than two minutes old (the streaming job has
// pushed a fresh window recently); otherwise it is red.
export function Header({ metrics }) {
  const latest = metrics && metrics.length > 0 ? metrics[metrics.length - 1] : null;
  const ageMs = latest ? Date.now() - parseUtc(latest.window_start).getTime() : Infinity;
  const isLive = ageMs < 2 * 60 * 1000;

  return (
    <header className="flex items-center justify-between">
      <h1 className="text-2xl font-semibold text-zinc-100">Performance Tracker</h1>
      <div className="flex items-center gap-2 text-sm text-zinc-400">
        <span
          className={`inline-block w-2 h-2 rounded-full transition-opacity ${
            isLive ? "bg-green-500" : "bg-red-500"
          }`}
        />
        <span>{isLive ? "live" : "offline"}</span>
      </div>
    </header>
  );
}
