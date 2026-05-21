// A single SVG heatmap of mouse traffic. One <rect> per cell from
// the API, filtered to the chosen event type ("move" or "click").
// Opacity scales as log(count + 1) / log(max + 1) so a cell with
// 5 hits is still visible next to one with 5000.
export function Heatmap({ data, type, title, color }) {
  const cells = (data || []).filter((c) => c.type === type);

  // viewBox is sized to fit the actual data with a 16:9 baseline so
  // an empty heatmap still has a sensible aspect ratio.
  const maxX = cells.reduce((m, c) => Math.max(m, c.cell_x + 1), 48);
  const maxY = cells.reduce((m, c) => Math.max(m, c.cell_y + 1), 27);
  const maxCount = cells.reduce((m, c) => Math.max(m, c.count), 0);
  const denom = Math.log(maxCount + 1);

  return (
    <div className="bg-zinc-800 rounded-lg p-4 border border-zinc-700">
      <h2 className="text-sm text-zinc-400 mb-3">{title}</h2>
      <svg
        viewBox={`0 0 ${maxX} ${maxY}`}
        preserveAspectRatio="xMidYMid meet"
        className="w-full bg-zinc-900 rounded"
      >
        {cells.map((c) => (
          <rect
            key={`${c.cell_x}-${c.cell_y}`}
            x={c.cell_x}
            y={c.cell_y}
            width={1}
            height={1}
            fill={color}
            opacity={denom > 0 ? Math.log(c.count + 1) / denom : 0}
          >
            <title>{`(${c.cell_x}, ${c.cell_y}) - ${c.count}`}</title>
          </rect>
        ))}
      </svg>
      {cells.length === 0 && (
        <div className="text-xs text-zinc-500 mt-2">
          no {type} events yet - move the mouse
        </div>
      )}
    </div>
  );
}
