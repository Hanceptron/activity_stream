import { pickHotspots } from "../utils";

// Top-5 click cells from the current heatmap range. Reuses the
// already user-filtered heatmap payload so the RangeSelector above
// implicitly controls this panel — switching from 1h to 1w changes
// both the heatmaps and the leaderboard.
//
// The bar in the right column is each cell's count relative to the
// top hotspot, giving a quick read on whether one location
// dominates or the activity is spread out.
export function HotspotsLeaderboard({ heatmap }) {
  const top = pickHotspots(heatmap, "click", 5);
  const topCount = top.length > 0 ? top[0].count ?? 0 : 0;

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <h2 className="text-sm text-zinc-400 mb-3">Click hotspots</h2>
      {top.length === 0 ? (
        <div className="text-xs text-zinc-500">
          no clicks recorded in this range
        </div>
      ) : (
        <div className="space-y-2">
          {top.map((cell, idx) => {
            const pct = topCount > 0 ? ((cell.count ?? 0) / topCount) * 100 : 0;
            return (
              <div
                key={`${cell.cell_x}-${cell.cell_y}-${idx}`}
                className="grid grid-cols-[24px_80px_60px_1fr] items-center gap-3 text-sm"
              >
                <div className="text-zinc-500 tabular-nums">{idx + 1}.</div>
                <div className="text-zinc-300 tabular-nums">
                  ({cell.cell_x}, {cell.cell_y})
                </div>
                <div className="text-zinc-200 tabular-nums text-right">
                  {cell.count}
                </div>
                <div
                  className="h-2 bg-zinc-700 rounded overflow-hidden"
                  aria-hidden="true"
                >
                  <div
                    className="h-full bg-red-400/70"
                    style={{ width: `${pct}%` }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
