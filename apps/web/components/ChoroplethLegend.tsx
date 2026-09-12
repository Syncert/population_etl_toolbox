// The one legend for a choropleth.
//
// A legend is the textual carrier of what colour alone must not carry
// (WEB-025): each bin's label, and how many geographies fell in it where
// that count is published. Both maps rendered this markup separately; a
// legend that drifted between them would make the same colour mean two
// things on two screens.

import type { LegendItem } from "../lib/explorerViewModel";

export default function ChoroplethLegend({
  title,
  items,
  ariaLabel,
  showCounts = false,
}: {
  title: string;
  items: LegendItem[];
  /** The accessible name of the legend region, stable for the browser suite. */
  ariaLabel: string;
  /** Whether each row names how many geographies it colours. */
  showCounts?: boolean;
}) {
  if (items.length === 0) {
    return null;
  }
  return (
    <div className="map-legend" aria-label={ariaLabel}>
      <div className="legend-title">{title}</div>
      {items.map((item) => (
        <div className="legend-row" key={`${item.color}-${item.label}`}>
          <span className="legend-swatch" style={{ backgroundColor: item.color }} />
          <span>
            {item.label}
            {showCounts && Number.isFinite(item.count) ? ` (${item.count})` : ""}
          </span>
        </div>
      ))}
    </div>
  );
}
