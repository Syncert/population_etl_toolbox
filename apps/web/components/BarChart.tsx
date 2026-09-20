// Published values as bars, in two shapes the workbench needs.
//
// *Over time* (`orientation="time"`): one bar per published period, grouped
// by series, with time on the horizontal axis. It is the presentation for a
// selection a line cannot draw — a measure with one published period has no
// line, and two points joined by a line imply a path between them that
// nothing was published for.
//
// *As a ranking* (`orientation="geography"`): one bar per geography at one
// grain, sorted by the chosen measure. That is the cross-sectional shape
// (WB-2), and it is here rather than in a second component because the two
// differ in what the category axis is, not in how a bar is drawn.
//
// Both obey the same rules as every other chart on this surface: a period or
// a geography that published no value is absent and counted, never a bar of
// height zero, because zero is a published value a county can really have.
// A bar's baseline is the axis minimum, which is zero for a scale that spans
// zero and the observed minimum otherwise — with the caption saying so,
// because a bar chart whose baseline is not zero exaggerates differences and
// saying nothing about it is the misleading part.

import type { AxisAssignment } from "../lib/workbench";
import { formatNumber } from "../lib/format";

const WIDTH = 680;
const HEIGHT = 320;
const PAD_LEFT = 56;
const PAD_RIGHT = 24;
const PAD_TOP = 20;
const PAD_BOTTOM = 56;

/** One bar: its category, its published value, and everything its tooltip says. */
export interface BarDatum {
  key: string;
  /** The category axis label — a period, or a geography's name. */
  category: string;
  value: number;
  unit: string;
  /** The series or measure this bar belongs to, for colour and the legend. */
  groupKey: string;
  groupLabel: string;
  /** The period this value describes, where the category is not already it. */
  period?: string;
  /** The release this value came from, where the source published one. */
  release?: string;
}

function formatValue(value: number): string {
  return formatNumber(value, { maximumFractionDigits: 3 });
}

/** What the category axis counts, singular or plural. */
function categoryNoun(orientation: "time" | "geography", count: number): string {
  if (orientation === "time") {
    return count === 1 ? "period" : "periods";
  }
  return count === 1 ? "geography" : "geographies";
}

/**
 * The value axis a set of bars is drawn against.
 *
 * Zero is included when the published values straddle it or approach it, so
 * the bars' lengths are proportional to the values. Where they do not — a
 * measure published between 61.2 and 63.4 — the axis starts at the observed
 * minimum, because a chart of four indistinguishable full-height bars
 * communicates nothing. Which of the two happened rides in `zeroBased` so the
 * caption can state it rather than leaving a reader to measure pixels.
 */
export function barValueScale(values: readonly number[]): {
  min: number;
  max: number;
  zeroBased: boolean;
} {
  if (values.length === 0) {
    return { min: 0, max: 1, zeroBased: true };
  }
  const observedMin = Math.min(...values);
  const observedMax = Math.max(...values);
  if (observedMin === observedMax) {
    // One distinct value: anchor at zero so the single bar has a length that
    // means something, unless the value itself is zero.
    return observedMin === 0
      ? { min: 0, max: 1, zeroBased: true }
      : { min: Math.min(0, observedMin), max: Math.max(0, observedMax), zeroBased: true };
  }
  const span = observedMax - observedMin;
  const straddlesZero = observedMin <= 0 && observedMax >= 0;
  const nearZero = Math.abs(observedMin) <= span;
  if (straddlesZero || nearZero) {
    return { min: Math.min(0, observedMin), max: Math.max(0, observedMax), zeroBased: true };
  }
  return { min: observedMin, max: observedMax, zeroBased: false };
}

export default function BarChart({
  bars,
  assignment,
  label,
  orientation = "time",
  colorOf,
  unpublished = 0,
  testId = "workbench-bar-chart",
}: {
  bars: BarDatum[];
  assignment?: AxisAssignment;
  /** The chart's accessible label; the caller composes it from the model. */
  label: string;
  orientation?: "time" | "geography";
  colorOf: (groupKey: string) => string;
  /** Categories that published no value: counted, never drawn as zero. */
  unpublished?: number;
  testId?: string;
}) {
  if (bars.length === 0) {
    return (
      <p className="subtle chart-empty" data-testid={`${testId}-empty`}>
        Nothing in this selection published a value, so there are no bars to
        draw. An unpublished value is not a value of zero.
      </p>
    );
  }

  const scale = barValueScale(bars.map((bar) => bar.value));
  const span = scale.max - scale.min || 1;
  const chartWidth = WIDTH - PAD_LEFT - PAD_RIGHT;
  const chartHeight = HEIGHT - PAD_TOP - PAD_BOTTOM;
  const slot = chartWidth / bars.length;
  const barWidth = Math.max(1, Math.min(28, slot * 0.7));
  const baselineY = PAD_TOP + ((scale.max - Math.max(scale.min, 0)) / span) * chartHeight;

  const groups = [...new Set(bars.map((bar) => bar.groupKey))];
  // Every category, or as many as fit without the labels overprinting each
  // other. Skipping labels is a rendering decision; skipping bars would be a
  // claim about the data, so bars are never skipped.
  const labelEvery = Math.max(1, Math.ceil(bars.length / 12));

  return (
    <figure className="line-chart" data-testid={testId} data-bar-count={bars.length}>
      <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={label}>
        <line
          className="chart-gridline"
          x1={PAD_LEFT}
          x2={WIDTH - PAD_RIGHT}
          y1={baselineY}
          y2={baselineY}
          stroke="currentColor"
          strokeOpacity="0.45"
        />
        {bars.map((bar, index) => {
          const valueY = PAD_TOP + ((scale.max - bar.value) / span) * chartHeight;
          const top = Math.min(valueY, baselineY);
          const height = Math.max(1, Math.abs(baselineY - valueY));
          const x = PAD_LEFT + index * slot + (slot - barWidth) / 2;
          return (
            <g key={bar.key} data-testid="workbench-bar" data-group-key={bar.groupKey}>
              <rect
                x={x}
                y={top}
                width={barWidth}
                height={height}
                fill={colorOf(bar.groupKey)}
                fillOpacity="0.85"
              >
                <title>
                  {`${bar.groupLabel} — ${bar.category}: ${formatValue(bar.value)} ${bar.unit}` +
                    (bar.period && bar.period !== bar.category ? ` (${bar.period})` : "") +
                    (bar.release ? `, release ${bar.release}` : "")}
                </title>
              </rect>
              {index % labelEvery === 0 ? (
                <text
                  x={x + barWidth / 2}
                  y={HEIGHT - PAD_BOTTOM + 16}
                  fontSize="10"
                  textAnchor="end"
                  fill="currentColor"
                  transform={`rotate(-40 ${x + barWidth / 2} ${HEIGHT - PAD_BOTTOM + 16})`}
                >
                  {bar.category}
                </text>
              ) : null}
            </g>
          );
        })}
        <text x={PAD_LEFT - 6} y={PAD_TOP + 4} fontSize="11" textAnchor="end" fill="currentColor">
          {formatValue(scale.max)}
        </text>
        <text
          x={PAD_LEFT - 6}
          y={PAD_TOP + chartHeight}
          fontSize="11"
          textAnchor="end"
          fill="currentColor"
        >
          {formatValue(scale.min)}
        </text>
      </svg>

      {groups.length > 1 ? (
        <ul className="chart-legend" data-testid="workbench-bar-legend">
          {groups.map((groupKey) => {
            const first = bars.find((bar) => bar.groupKey === groupKey);
            return (
              <li key={groupKey} data-series-key={groupKey}>
                <span
                  aria-hidden="true"
                  className="legend-swatch"
                  style={{ backgroundColor: colorOf(groupKey) }}
                />
                <span>{first?.groupLabel || groupKey}</span>
              </li>
            );
          })}
        </ul>
      ) : null}

      <figcaption className="subtle">
        {orientation === "time"
          ? "Each bar is one published period."
          : "Each bar is one geography's own newest published value."}{" "}
        {scale.zeroBased ? (
          "The value axis includes zero, so the bars' lengths are proportional."
        ) : (
          <strong data-testid="bar-baseline-note">
            {`The value axis starts at ${formatValue(scale.min)}, not at zero, because the published values do not approach it — compare the bars' tops rather than their lengths.`}
          </strong>
        )}
        {assignment?.note ? ` ${assignment.note}` : ""}
        {unpublished > 0 ? (
          <>
            {" "}
            <strong data-testid="bar-unpublished">
              {`${unpublished} ${categoryNoun(orientation, unpublished)} published no value and ${unpublished === 1 ? "is" : "are"} not drawn.`}
            </strong>{" "}
            An unpublished value is not a value of zero.
          </>
        ) : null}
      </figcaption>
    </figure>
  );
}
