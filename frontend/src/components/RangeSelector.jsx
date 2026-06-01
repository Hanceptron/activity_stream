// Small horizontal button group for selecting the heatmap time
// range. The five values correspond exactly to the directories the
// batch job writes under output/heatmaps/.
const RANGES = [
  ["1h", "1 h"],
  ["6h", "6 h"],
  ["1d", "1 d"],
  ["3d", "3 d"],
  ["1w", "1 w"],
];

export function RangeSelector({ value, onChange }) {
  return (
    <div className="inline-flex rounded-md bg-zinc-800/60 border border-white/10 overflow-hidden backdrop-blur">
      {RANGES.map(([v, label]) => (
        <button
          key={v}
          onClick={() => onChange(v)}
          className={`px-3 py-1 text-xs transition-colors ${
            value === v
              ? "bg-brand-violet/30 text-zinc-100"
              : "text-zinc-400 hover:bg-white/10 hover:text-zinc-200"
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
