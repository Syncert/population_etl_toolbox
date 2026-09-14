// The pairwise correlation matrix, as a heatmap with a diverging scale.
//
// Diverging, not sequential, because a correlation has a meaningful middle. A
// sequential ramp would make −0.8 and +0.8 the two ends of one gradient and
// zero a midtone, which reads as "0.8 is a lot of something and −0.8 is none
// of it" — the opposite of what the numbers say. Zero is the neutral colour
// and the two directions run away from it.
//
// Three kinds of cell are deliberately not on the scale:
//
// - **The diagonal**, where a measure meets itself. Perfect by arithmetic,
//   not by measurement, so it carries no coefficient — a grid whose diagonal
//   reads 1.000 invites the eye to calibrate the rest against a number
//   nothing measured.
// - **A declined pair**, which the compatibility policy refused. Drawn in the
//   not-published colour with its failed rule in the tooltip.
// - **A comparable pair whose coefficient the data cannot carry** — fewer
//   than three paired geographies, or a constant side. The API answers `null`
//   there rather than `0`, and so does this.
//
// All three are hatched as well as coloured, because colour is never the only
// carrier of a distinction (WEB-025), and because "declined" and "near zero"
// must never be one glance apart.

import ChoroplethLegend from "./ChoroplethLegend";
import { CHOROPLETH_WITHHELD_COLOR } from "../lib/explorerViewModel";
import type { LegendItem } from "../lib/explorerViewModel";
import { formatCoefficient } from "../lib/workbench";
import type { CorrelationMatrixModel } from "../lib/workbench";

const CELL = 44;
const GAP = 2;
const ROW_LABEL = 168;
const TOP_LABEL = 96;

/**
 * The diverging ramp, from strong negative through neutral to strong
 * positive. Fixed to the coefficient's own domain of −1 to 1 rather than to
 * the observed range: a matrix whose strongest cell is 0.3 must not paint it
 * like a 0.95, and a scale that rescales itself per answer would do exactly
 * that.
 */
export const CORRELATION_DIVERGING_SCALE = [
  { from: -1.0, to: -0.6, color: "#2f5d8c", label: "−1.00 to −0.60" },
  { from: -0.6, to: -0.2, color: "#7fa6c4", label: "−0.60 to −0.20" },
  { from: -0.2, to: 0.2, color: "#eee9e0", label: "−0.20 to 0.20" },
  { from: 0.2, to: 0.6, color: "#d6a069", label: "0.20 to 0.60" },
  { from: 0.6, to: 1.0, color: "#a3552a", label: "0.60 to 1.00" },
] as const;

export function correlationColor(value: number): string {
  for (const band of CORRELATION_DIVERGING_SCALE) {
    if (value >= band.from && value <= band.to) {
      return band.color;
    }
  }
  return CHOROPLETH_WITHHELD_COLOR;
}

function legendItems(model: CorrelationMatrixModel): LegendItem[] {
  const items: LegendItem[] = CORRELATION_DIVERGING_SCALE.map((band) => ({
    color: band.color,
    label: band.label,
  }));
  if (model.declinedCount > 0) {
    items.push({
      color: CHOROPLETH_WITHHELD_COLOR,
      label: "Pair declined by the compatibility policy",
      count: model.declinedCount,
    });
  }
  return items;
}

export default function CorrelationMatrixChart({
  model,
  labelFor,
  which,
  testId = "workbench-correlation-matrix",
}: {
  model: CorrelationMatrixModel;
  /** A measure's display label, from the catalog. */
  labelFor: (metricCode: string) => string;
  which: "pearson_r" | "spearman_rho";
  testId?: string;
}) {
  if (model.codes.length === 0) {
    return null;
  }

  const width = ROW_LABEL + model.codes.length * (CELL + GAP);
  const height = TOP_LABEL + model.codes.length * (CELL + GAP);
  const indexOf = new Map(model.codes.map((code, index) => [code, index]));
  const coefficient = which === "pearson_r" ? "Pearson r" : "Spearman ρ";

  const label =
    `Correlation matrix of ${model.codes.length} measures, ${coefficient}. ` +
    `${model.measuredCount} pairs carry a coefficient and ` +
    `${model.declinedCount} were declined by the compatibility policy. ` +
    "Every coefficient here is API-derived. The readings below the matrix " +
    "list each pair.";

  return (
    <figure
      className="line-chart"
      data-testid={testId}
      data-measured-count={model.measuredCount}
      data-declined-count={model.declinedCount}
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
            <pattern
              id="workbench-correlation-absent"
              width="5"
              height="5"
              patternUnits="userSpaceOnUse"
              patternTransform="rotate(45)"
            >
              <rect width="5" height="5" fill={CHOROPLETH_WITHHELD_COLOR} />
              <line
                x1="0"
                y1="0"
                x2="0"
                y2="5"
                stroke="currentColor"
                strokeOpacity="0.45"
                strokeWidth="1.6"
              />
            </pattern>
          </defs>

          {model.codes.map((code, index) => (
            <text
              key={`row-${code}`}
              x={ROW_LABEL - 8}
              y={TOP_LABEL + index * (CELL + GAP) + CELL / 2 + 4}
              fontSize="11"
              textAnchor="end"
              fill="currentColor"
            >
              {labelFor(code)}
            </text>
          ))}

          {model.codes.map((code, index) => (
            <text
              key={`column-${code}`}
              x={ROW_LABEL + index * (CELL + GAP) + CELL / 2}
              y={TOP_LABEL - 8}
              fontSize="11"
              textAnchor="start"
              fill="currentColor"
              transform={`rotate(-55 ${
                ROW_LABEL + index * (CELL + GAP) + CELL / 2
              } ${TOP_LABEL - 8})`}
            >
              {labelFor(code)}
            </text>
          ))}

          {model.cells.map((cell) => {
            const row = indexOf.get(cell.metricCodeA) ?? 0;
            const column = indexOf.get(cell.metricCodeB) ?? 0;
            const measured = cell.value !== null;
            const x = ROW_LABEL + column * (CELL + GAP);
            const y = TOP_LABEL + row * (CELL + GAP);
            return (
              <g
                key={`${cell.metricCodeA}-${cell.metricCodeB}`}
                data-testid={
                  cell.declined
                    ? "correlation-cell-declined"
                    : cell.identity
                      ? "correlation-cell-identity"
                      : measured
                        ? "correlation-cell"
                        : "correlation-cell-unmeasured"
                }
                data-metric-a={cell.metricCodeA}
                data-metric-b={cell.metricCodeB}
              >
                <rect
                  x={x}
                  y={y}
                  width={CELL}
                  height={CELL}
                  fill={
                    measured
                      ? correlationColor(cell.value as number)
                      : "url(#workbench-correlation-absent)"
                  }
                >
                  <title>
                    {measured
                      ? `${labelFor(cell.metricCodeA)} against ${labelFor(
                          cell.metricCodeB,
                        )}: ${coefficient} ${formatCoefficient(
                          cell.value,
                        )} over ${cell.n.toLocaleString()} paired geographies. API-derived.`
                      : `${labelFor(cell.metricCodeA)} against ${labelFor(
                          cell.metricCodeB,
                        )}: ${cell.reason}`}
                  </title>
                </rect>
                {measured ? (
                  <text
                    x={x + CELL / 2}
                    y={y + CELL / 2 + 4}
                    fontSize="10"
                    textAnchor="middle"
                    fill="currentColor"
                  >
                    {formatCoefficient(cell.value)}
                  </text>
                ) : null}
              </g>
            );
          })}
        </svg>
      </div>

      <ChoroplethLegend
        title={`${coefficient} (API-derived)`}
        items={legendItems(model)}
        ariaLabel={`Colour scale for the ${coefficient} matrix`}
        showCounts
      />

      <figcaption className="subtle">
        The scale is fixed to a coefficient&apos;s own range of −1 to 1, so a
        matrix whose strongest pair is weak is not painted as though it were
        strong. The diagonal carries no coefficient — a measure is perfectly
        correlated with itself by arithmetic, not by measurement. A declined
        pair and a pair whose data cannot carry a coefficient are hatched
        rather than coloured; neither is a coefficient near zero.
      </figcaption>
    </figure>
  );
}
