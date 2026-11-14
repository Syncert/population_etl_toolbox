// A headline card's levels on one scale (web-level-strip). Loaded with its
// own chunk so the place-page bundle carries only the import.
//
// The card's table stays in the page for screen readers and copy; the strip
// is a picture of the same rows, and the sentence under it is the reading.

import { formatObservationValue } from "../lib/explorerViewModel";
import { DISPLAY_LOCALE } from "../lib/format";
import { levelStrip } from "../lib/levelStrip";
import type { CardRow } from "../lib/placeChapters";

/** The trend charts' colours, in the same level order (PlaceTrend). */
const COLOURS = ["#0b6b57", "#8c3b17", "#2b4c8c", "#5b6872"] as const;
/** Axis ticks are short ("200K") so they never run into each other. */
const TICK = new Intl.NumberFormat(DISPLAY_LOCALE, { notation: "compact", maximumFractionDigits: 1 });

export default function PlaceLevelStrip({ measureId, rows }: { measureId: string; rows: CardRow[] }) {
  const strip = levelStrip(measureId, rows);
  if (!strip) return null;
  const [lo, hi] = strip.domain;
  const at = (value: number) => `${(((value - lo) / (hi - lo)) * 100).toFixed(2)}%`;
  const span = (value: number, margin: number) => `${(((2 * margin) / (hi - lo)) * 100).toFixed(2)}%`;
  return (
    <div className="level-strip" data-testid={`card-${measureId}-strip`}>
      <div aria-hidden="true">
        {strip.points.map((point) => (
          <div className="level-strip-row" key={point.name} data-index={point.index}>
            <span className="level-strip-name">{point.name}</span>
            <span className="level-strip-track">
              {point.margin ? (
                <i className="level-strip-moe" style={{ left: at(point.value - point.margin), width: span(point.value, point.margin), color: COLOURS[point.index % 4] }} />
              ) : null}
              <i className="level-strip-dot" style={{ left: at(point.value), background: COLOURS[point.index % 4] }} />
            </span>
            <span className="level-strip-value">
              {formatObservationValue(point.value)}
              {point.margin ? <small> ±{formatObservationValue(point.margin)}</small> : null}
            </span>
          </div>
        ))}
        <div className="level-strip-row level-strip-axis">
          <span />
          <span className="level-strip-track">
            {strip.ticks.map((tick) => (
              <small key={tick} style={{ left: at(tick) }}>{TICK.format(tick)}</small>
            ))}
          </span>
          <span />
        </div>
      </div>
      {strip.sentence ? <p className="level-strip-sentence" data-testid={`card-${measureId}-strip-sentence`}>{strip.sentence}</p> : null}
    </div>
  );
}
