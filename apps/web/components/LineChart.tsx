// Several published measures on one time axis, each at one geography.
//
// The workbench's longitudinal chart. It follows `TimeSeriesChart`'s rules
// rather than restating them, because they are the honesty rules and there
// must be one set of them:
//
// - Time is the horizontal axis, positioned by date. Placing points by index
//   closes the gap the API left open on purpose: a series missing 1980 would
//   draw 1979 and 1981 adjacent, and the line between them would slope as
//   though the measure had moved across one ordinary interval (WEB-042).
// - A period that published no value is not drawn and not coerced. It is
//   counted, and the count is on the chart.
// - Nothing is normalised. Two units mean two axes with their own scales,
//   and the caption says the lines' relative heights carry no meaning.
//
// What it adds is the part a multi-series chart needs: a legend that names
// each series' publisher, measure, grain and geography, and a data table so
// every plotted value is readable without the chart.

import type { AxisAssignment, PlottedSeries } from "../lib/workbench";
import { describeChart, describeSeries } from "../lib/workbench";

const WIDTH = 680;
const HEIGHT = 320;
const PAD_LEFT = 56;
const PAD_RIGHT = 56;
const PAD_TOP = 20;
const PAD_BOTTOM = 44;

/**
 * The stroke each series is drawn in.
 *
 * Colour is never the only carrier of the distinction: the legend repeats
 * each series' colour as a swatch beside its full sentence, the data table
 * below carries every value, and each path declares its own
 * `data-series-key`. Six because a seventh and eighth series reuse the first
 * two strokes with a dashed pattern, which is a second distinction rather
 * than a seventh indistinguishable hue.
 */
const STROKES = [
  "#0b6b57",
  "#8c3b17",
  "#2b4c8c",
  "#6b2b6b",
  "#3f6b1f",
  "#8c6b0b",
] as const;

export function seriesStroke(index: number): string {
  return STROKES[index % STROKES.length] as string;
}

export function seriesDashed(index: number): boolean {
  return index >= STROKES.length;
}

function formatValue(value: number): string {
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 3 });
}

interface AxisScale {
  min: number;
  max: number;
}

function axisScale(
  plotted: readonly PlottedSeries[],
  keys: readonly string[],
): AxisScale {
  const values = plotted
    .filter((entry) => keys.includes(entry.key))
    .flatMap((entry) => entry.points.map((point) => point.value));
  if (values.length === 0) {
    return { min: 0, max: 1 };
  }
  const min = Math.min(...values);
  const max = Math.max(...values);
  // A flat series still draws: a zero span would divide by zero and put every
  // point at the same coordinate with no scale to read it against.
  return min === max ? { min, max: min + 1 } : { min, max };
}

function positionY(value: number, scale: AxisScale): number {
  const span = scale.max - scale.min;
  const chartHeight = HEIGHT - PAD_TOP - PAD_BOTTOM;
  return PAD_TOP + ((scale.max - value) / span) * chartHeight;
}

export default function LineChart({
  plotted,
  assignment,
  geographyNames = {},
  testId = "workbench-line-chart",
}: {
  plotted: PlottedSeries[];
  assignment: AxisAssignment;
  geographyNames?: Record<string, string>;
  testId?: string;
}) {
  const drawable = plotted.filter((entry) => entry.points.length > 0);
  if (drawable.length === 0) {
    return (
      <p className="subtle chart-empty" data-testid={`${testId}-empty`}>
        None of the selected series published a value over this period. An
        unpublished period is not a period with a value of zero.
      </p>
    );
  }

  // One time domain for the whole chart, so two series covering different
  // spans sit in their real positions relative to each other rather than each
  // being stretched to the full width.
  const times = drawable
    .flatMap((entry) => entry.points.map((point) => point.time))
    .filter((time): time is number => time !== null);
  const minTime = times.length > 0 ? Math.min(...times) : 0;
  const maxTime = times.length > 0 ? Math.max(...times) : 0;
  const timeSpan = maxTime - minTime;
  const chartWidth = WIDTH - PAD_LEFT - PAD_RIGHT;

  const scaleOf = new Map(
    assignment.axes.map((axis) => [axis.unit, axisScale(plotted, axis.seriesKeys)]),
  );
  const axisOf = new Map<string, string>();
  for (const axis of assignment.axes) {
    for (const key of axis.seriesKeys) {
      axisOf.set(key, axis.unit);
    }
  }

  function positionX(time: number | null, index: number, count: number): number {
    if (time === null || timeSpan <= 0) {
      return PAD_LEFT + (count <= 1 ? chartWidth / 2 : (index / (count - 1)) * chartWidth);
    }
    return PAD_LEFT + ((time - minTime) / timeSpan) * chartWidth;
  }

  return (
    <figure className="line-chart" data-testid={testId} data-series-count={drawable.length}>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={describeChart("line", plotted)}>
        <line
          className="chart-gridline"
          x1={PAD_LEFT}
          x2={WIDTH - PAD_RIGHT}
          y1={HEIGHT - PAD_BOTTOM}
          y2={HEIGHT - PAD_BOTTOM}
          stroke="currentColor"
          strokeOpacity="0.35"
        />
        <line
          className="chart-gridline"
          x1={PAD_LEFT}
          x2={PAD_LEFT}
          y1={PAD_TOP}
          y2={HEIGHT - PAD_BOTTOM}
          stroke="currentColor"
          strokeOpacity="0.35"
        />
        {drawable.map((entry, seriesIndex) => {
          const scale = scaleOf.get(axisOf.get(entry.key) || "") || { min: 0, max: 1 };
          const points = entry.points.map((point, index) => ({
            ...point,
            x: positionX(point.time, index, entry.points.length),
            y: positionY(point.value, scale),
          }));
          const stroke = seriesStroke(seriesIndex);
          return (
            <g key={entry.key} data-series-key={entry.key}>
              {points.length > 1 ? (
                <polyline
                  className="chart-line"
                  data-testid="workbench-line"
                  fill="none"
                  stroke={stroke}
                  strokeWidth="1.8"
                  strokeDasharray={seriesDashed(seriesIndex) ? "5 3" : undefined}
                  points={points.map((point) => `${point.x},${point.y}`).join(" ")}
                />
              ) : null}
              {points.map((point) => (
                <circle
                  key={`${entry.key}-${point.period}`}
                  className="chart-point"
                  cx={point.x}
                  cy={point.y}
                  r="3"
                  fill={stroke}
                >
                  <title>{`${entry.label} — ${point.period}: ${formatValue(point.value)} ${entry.unit}`}</title>
                </circle>
              ))}
            </g>
          );
        })}
        {assignment.axes.slice(0, 2).map((axis, index) => {
          const scale = scaleOf.get(axis.unit) || { min: 0, max: 1 };
          const x = index === 0 ? PAD_LEFT - 6 : WIDTH - PAD_RIGHT + 6;
          const anchor = index === 0 ? "end" : "start";
          return (
            <g key={axis.unit} data-testid="workbench-axis" data-axis-unit={axis.unit}>
              <text x={x} y={PAD_TOP + 4} fontSize="11" textAnchor={anchor} fill="currentColor">
                {formatValue(scale.max)}
              </text>
              <text
                x={x}
                y={HEIGHT - PAD_BOTTOM}
                fontSize="11"
                textAnchor={anchor}
                fill="currentColor"
              >
                {formatValue(scale.min)}
              </text>
              <text
                x={x}
                y={HEIGHT - PAD_BOTTOM + 16}
                fontSize="10"
                textAnchor={anchor}
                fill="currentColor"
                opacity="0.8"
              >
                {axis.unit}
              </text>
            </g>
          );
        })}
      </svg>

      <ul className="chart-legend" data-testid="workbench-legend">
        {drawable.map((entry, seriesIndex) => (
          <li key={entry.key} data-series-key={entry.key}>
            <span
              aria-hidden="true"
              className="legend-swatch"
              style={{ backgroundColor: seriesStroke(seriesIndex) }}
            />
            <span>
              {describeSeries(entry, geographyNames[entry.series.geoId])} — {entry.unit}
            </span>
            {entry.droppedPeriods > 0 ? (
              <span className="subtle" data-testid="legend-dropped-periods">
                {` ${entry.droppedPeriods} period${entry.droppedPeriods === 1 ? "" : "s"} published no value.`}
              </span>
            ) : null}
            {entry.truncated ? (
              <span className="subtle" data-testid="legend-truncated">
                {" The page bound cut this history short, so it is a prefix."}
              </span>
            ) : null}
          </li>
        ))}
      </ul>

      <figcaption className="subtle">
        {assignment.note ? <span data-testid="workbench-axis-note">{assignment.note} </span> : null}
        Every plotted value is listed in the table below.
      </figcaption>
    </figure>
  );
}
