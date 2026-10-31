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
  note,
}: {
  title: string;
  items: LegendItem[];
  /** The accessible name of the legend region, stable for the browser suite. */
  ariaLabel: string;
  /** Whether each row names how many geographies it colours. */
  showCounts?: boolean;
  /**
   * A line under the title naming what the map is drawn on, shown also as the
   * title's tooltip. `marked` puts an asterisk on the title pointing at it,
   * for boundaries from a year other than the values' (web-place-maps).
   */
  note?: { text: string; marked: boolean } | null;
}) {
  if (items.length === 0) {
    return null;
  }
  return (
    <div className="map-legend" aria-label={ariaLabel}>
      <div className="legend-title" title={note?.text}>
        {title}
        {note?.marked ? <span aria-hidden="true"> *</span> : null}
      </div>
      {note ? (
        <div className="legend-note" data-testid="map-legend-boundary" data-marked={note.marked ? "true" : "false"} title={note.text}>
          {note.marked ? "* " : ""}
          {note.text}
        </div>
      ) : null}
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
