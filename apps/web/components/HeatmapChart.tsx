// One measure laid out as geographies down and periods across.
//
// The presentation the plan separates from the *correlation* heatmap on
// purpose: this one is a rendering of published values and needs only
// `/observations`, while a correlation matrix is a derived analysis with
// caveats. They share this component and differ in what they hand it.
//
// The honesty rules are the choropleth's, one layer over:
//
// - A cell the measure published no number for is drawn in a colour that is
//   never on the scale, and carries a hatch as well, because colour is never
//   the only carrier of a distinction. Where the source published a reason —
//   a CDC suppression, a NASS `(D)` — the tooltip says it in the source's own
//   word. A withheld value, no observation, and a low value are three
//   different statements (WEB-078).
// - The legend is `ChoroplethLegend`, so a swatch means the same thing here
//   as it does on a map.
// - Nothing is interpolated. A cell is one published value or it is not a
//   value, and no cell is filled in from its neighbours.

import ChoroplethLegend from "./ChoroplethLegend";
import {
  CHOROPLETH_PALETTE,
  CHOROPLETH_WITHHELD_COLOR,
} from "../lib/explorerViewModel";
import type { LegendItem } from "../lib/explorerViewModel";
import type { HeatmapModel } from "../lib/workbench";

const CELL = 16;
const GAP = 1;
const ROW_LABEL = 128;
const TOP_LABEL = 78;

function formatValue(value: number): string {
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 3 });
}

/**
 * The scale colour for one value.
 *
 * Equal-width over the observed range, the same shape `colorForValue` gives
 * the choropleth — and deliberately not `/distribution/bins`, which reduces
 * each geography to its own newest period. A heatmap draws every period, so
 * binning it against a single-period distribution would colour most of the
 * picture against a range it was not measured over.
 */
export function heatmapColor(
  value: number,
  minValue: number,
  maxValue: number,
): string {
  const span = maxValue - minValue;
  const ratio = span <= 0 ? 0 : (value - minValue) / span;
  const index = Math.max(
    0,
    Math.min(
      CHOROPLETH_PALETTE.length - 1,
      Math.floor(ratio * CHOROPLETH_PALETTE.length),
    ),
  );
  return CHOROPLETH_PALETTE[index] as string;
}

function legendItems(model: HeatmapModel, unit: string): LegendItem[] {
  if (model.minValue === null || model.maxValue === null) {
    return [];
  }
  const span = model.maxValue - model.minValue;
  const items: LegendItem[] = CHOROPLETH_PALETTE.map((color, index) => {
    const from = model.minValue! + (span * index) / CHOROPLETH_PALETTE.length;
    const to = model.minValue! + (span * (index + 1)) / CHOROPLETH_PALETTE.length;
    return {
      color,
      label:
        span <= 0
          ? `${formatValue(model.minValue!)} ${unit}`
          : `${formatValue(from)} – ${formatValue(to)} ${unit}`,
    };
  });
  if (model.unpublishedCount > 0) {
    items.push({
      color: CHOROPLETH_WITHHELD_COLOR,
      label: "No published value",
      count: model.unpublishedCount,
    });
  }
  return items;
}

export default function HeatmapChart({
  model,
  measureLabel,
  unit,
  testId = "workbench-heatmap",
}: {
  model: HeatmapModel;
  measureLabel: string;
  unit: string;
  testId?: string;
}) {
  if (model.geographies.length === 0 || model.periods.length === 0) {
    return (
      <p className="subtle chart-empty" data-testid={`${testId}-empty`}>
        This measure published no values to lay out. An empty answer is not a
        grid of zeroes.
      </p>
    );
  }

  const width = ROW_LABEL + model.periods.length * (CELL + GAP);
  const height = TOP_LABEL + model.geographies.length * (CELL + GAP);
  const rowOf = new Map(
    model.geographies.map((geography, index) => [geography.geoId, index]),
  );
  const columnOf = new Map(
    model.periods.map((period, index) => [period, index]),
  );
  // Every nth column labelled, so the labels do not overprint. Never every nth
  // *cell*: skipping a label is a rendering decision, skipping a cell would be
  // a claim about the data.
  const labelEvery = Math.max(1, Math.ceil(model.periods.length / 14));

  const label =
    `Heatmap of ${measureLabel} in ${unit}: ${model.geographies.length} ` +
    `geographies down, ${model.periods.length} periods across. ` +
    `${model.valueCount} cells carry a published value and ` +
    `${model.unpublishedCount} published none.` +
    (model.capped ? ` ${model.capNote}` : "") +
    " The table below the chart lists every published cell.";

  return (
    <figure
      className="line-chart"
      data-testid={testId}
      data-geography-count={model.geographies.length}
      data-period-count={model.periods.length}
      data-unpublished-count={model.unpublishedCount}
    >
      <div className="table-wrap">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          width={width}
          height={height}
          role="img"
          aria-label={label}
        >
          <defs>
            {/* The second carrier of the "not published" distinction: a
                reader who cannot separate the withheld colour from the low
                end of the scale still sees the hatch. */}
            <pattern
              id="workbench-heatmap-unpublished"
              width="4"
              height="4"
              patternUnits="userSpaceOnUse"
              patternTransform="rotate(45)"
            >
              <rect width="4" height="4" fill={CHOROPLETH_WITHHELD_COLOR} />
              <line
                x1="0"
                y1="0"
                x2="0"
                y2="4"
                stroke="currentColor"
                strokeOpacity="0.45"
                strokeWidth="1.5"
              />
            </pattern>
          </defs>

          {model.geographies.map((geography, index) => (
            <text
              key={geography.geoId}
              x={ROW_LABEL - 6}
              y={TOP_LABEL + index * (CELL + GAP) + CELL - 4}
              fontSize="10"
              textAnchor="end"
              fill="currentColor"
            >
              {geography.name}
            </text>
          ))}

          {model.periods.map((period, index) =>
            index % labelEvery === 0 ? (
              <text
                key={period}
                x={ROW_LABEL + index * (CELL + GAP) + CELL / 2}
                y={TOP_LABEL - 6}
                fontSize="10"
                textAnchor="start"
                fill="currentColor"
                transform={`rotate(-60 ${
                  ROW_LABEL + index * (CELL + GAP) + CELL / 2
                } ${TOP_LABEL - 6})`}
              >
                {period}
              </text>
            ) : null,
          )}

          {model.cells.map((cell) => {
            const row = rowOf.get(cell.geoId) ?? 0;
            const column = columnOf.get(cell.period) ?? 0;
            const published = cell.value !== null;
            const name =
              model.geographies.find(
                (geography) => geography.geoId === cell.geoId,
              )?.name || cell.geoId;
            return (
              <rect
                key={`${cell.geoId}-${cell.period}`}
                data-testid={
                  published ? "heatmap-cell" : "heatmap-cell-unpublished"
                }
                data-geo-id={cell.geoId}
                data-period={cell.period}
                x={ROW_LABEL + column * (CELL + GAP)}
                y={TOP_LABEL + row * (CELL + GAP)}
                width={CELL}
                height={CELL}
                fill={
                  published
                    ? heatmapColor(
                        cell.value as number,
                        model.minValue as number,
                        model.maxValue as number,
                      )
                    : "url(#workbench-heatmap-unpublished)"
                }
              >
                <title>
                  {published
                    ? `${name} — ${cell.period}: ${formatValue(
                        cell.value as number,
                      )} ${unit}${cell.release ? `, release ${cell.release}` : ""}`
                    : `${name} — ${cell.period}: no published value${
                        cell.valueStatus ? ` (${cell.valueStatus})` : ""
                      }. Not a value of zero.`}
                </title>
              </rect>
            );
          })}
        </svg>
      </div>

      <ChoroplethLegend
        title={`${measureLabel} (${unit})`}
        items={legendItems(model, unit)}
        ariaLabel={`Colour scale for ${measureLabel}`}
        showCounts
      />

      <figcaption className="subtle">
        Each cell is one geography&apos;s published value for one period.
        Nothing is interpolated between cells, and a cell with no published
        value is hatched rather than coloured — it is not a value of zero and
        not the low end of the scale.
        {model.capped ? (
          <>
            {" "}
            <strong data-testid="heatmap-cap-note">{model.capNote}</strong>
          </>
        ) : null}
      </figcaption>
    </figure>
  );
}
