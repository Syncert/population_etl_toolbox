// A scatter of two published measures, one point per geography.
//
// This is the aligned chart for a two-measure comparison: it needs no shared
// axis or unit, and each point is one geography's own pair rather than a
// series that would imply the two measures share a scale. It is never the
// only way to read a value — the comparison table beside it carries every
// row — and it says how many geographies it could not plot rather than
// quietly dropping them.

import type { ScatterModel } from "../lib/comparison";

const WIDTH = 620;
const HEIGHT = 320;
const PAD = 48;

function scale(value: number, min: number, max: number, from: number, to: number): number {
  const span = max - min;
  if (!Number.isFinite(span) || span === 0) {
    return (from + to) / 2;
  }
  return from + ((value - min) / span) * (to - from);
}

function axisLabel(value: number): string {
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
}

export default function ScatterChart({
  model,
  labelX,
  labelY,
  testId = "comparison-scatter",
}: {
  model: ScatterModel;
  labelX: string;
  labelY: string;
  testId?: string;
}) {
  const { points, minX, maxX, minY, maxY } = model;

  return (
    <figure className="line-chart" data-testid={testId} data-point-count={points.length}>
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`Scatter plot of ${points.length} geographies: ${labelX} on the horizontal axis against ${labelY} on the vertical axis. The comparison table below lists every value.`}
      >
        <line
          className="dash-gridline"
          x1={PAD}
          x2={WIDTH - PAD}
          y1={HEIGHT - PAD}
          y2={HEIGHT - PAD}
          stroke="currentColor"
          strokeOpacity="0.35"
        />
        <line
          className="dash-gridline"
          x1={PAD}
          x2={PAD}
          y1={PAD / 2}
          y2={HEIGHT - PAD}
          stroke="currentColor"
          strokeOpacity="0.35"
        />
        {points.map((point) => (
          <circle
            key={`${point.geoId}-${point.x}-${point.y}`}
            cx={scale(point.x, minX, maxX, PAD, WIDTH - PAD)}
            cy={scale(point.y, minY, maxY, HEIGHT - PAD, PAD / 2)}
            r="4"
            fill="#0b6b57"
            fillOpacity="0.75"
          >
            <title>{`${point.name}: ${axisLabel(point.x)}, ${axisLabel(point.y)}`}</title>
          </circle>
        ))}
        <text x={PAD} y={HEIGHT - PAD + 18} fontSize="11" fill="currentColor">
          {axisLabel(minX)}
        </text>
        <text
          x={WIDTH - PAD}
          y={HEIGHT - PAD + 18}
          fontSize="11"
          textAnchor="end"
          fill="currentColor"
        >
          {axisLabel(maxX)}
        </text>
        <text x={PAD - 6} y={PAD / 2 + 4} fontSize="11" textAnchor="end" fill="currentColor">
          {axisLabel(maxY)}
        </text>
        <text x={PAD - 6} y={HEIGHT - PAD} fontSize="11" textAnchor="end" fill="currentColor">
          {axisLabel(minY)}
        </text>
      </svg>
      <figcaption className="subtle">
        Horizontal: {labelX}. Vertical: {labelY}. Each point is one geography&apos;s own pair
        of published values.
        {model.excluded > 0 ? (
          <>
            {" "}
            <strong data-testid="scatter-excluded">
              {model.excluded} geograph{model.excluded === 1 ? "y is" : "ies are"} not plotted
              because one side published no usable value.
            </strong>{" "}
            They remain in the table below.
          </>
        ) : null}
      </figcaption>
    </figure>
  );
}
