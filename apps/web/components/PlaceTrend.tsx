// One chapter's trend: this place as the primary line, its parents as
// reference lines.
//
// The rules are `TimeSeriesChart`'s: time is positioned by date, a period
// without a published number is not drawn and is counted, and every plotted
// value is readable in the table below the chart. The only transformation is
// `buildTrend`'s index for count measures, whose base period the caption
// states. A published margin of error is drawn as a bar through its point and
// stated beside its value in the table (web-city-history); a period one place
// did not publish is named as such, never drawn as a gap filled in.

import type { TrendModel } from "../lib/placeChapters";
import { formatNumber } from "../lib/format";

const WIDTH = 640;
const HEIGHT = 220;
const PAD_LEFT = 52;
/** About one 11px digit's width: the value labels set the left margin. */
const TICK_CHAR_WIDTH = 6.6;
const PAD_RIGHT = 16;
const PAD_TOP = 14;
const PAD_BOTTOM = 34;

const STROKES = ["#0b6b57", "#8c3b17", "#2b4c8c"] as const;
const DASHES = ["", "6 4", "2 3"] as const;

function format(value: number): string {
  return formatNumber(value, { maximumFractionDigits: 2 });
}

/** An axis label: a year, or a five-year span's first and last year. */
function tick(period: string | undefined): string {
  if (!period) return "";
  return /^\d{4}(–\d{4})?$/.test(period) ? period : period.slice(0, 4);
}

function listNames(names: string[]): string {
  if (names.length <= 1) return names.join("");
  return `${names.slice(0, -1).join(", ")} and ${names.at(-1)}`;
}

export default function PlaceTrend({
  model,
  label,
  unit,
  notes = [],
  testId,
}: {
  model: TrendModel;
  label: string;
  unit: string;
  /** What the chart says about its own measure: margins, overlapping periods. */
  notes?: string[];
  testId: string;
}) {
  const drawn = model.lines.filter((line) => line.points.length > 0);
  if (drawn.length === 0) {
    return (
      <p className="subtle chart-empty" data-testid={`${testId}-empty`}>
        No published history to draw for {label}.
      </p>
    );
  }
  const times = drawn.flatMap((line) => line.points.map((point) => point.time));
  // Bars are drawn on published values only: an index divides by a base that
  // carries its own margin, so a scaled bar would claim a precision it lacks.
  const bars = model.scale === "level";
  const values = drawn.flatMap((line) =>
    line.points.flatMap((point) =>
      bars && point.moe !== null ? [point.value - point.moe, point.value + point.moe] : [point.value],
    ),
  );
  const hasBars = bars && drawn.some((line) => line.points.some((point) => point.moe !== null));
  const minTime = Math.min(...times);
  const timeSpan = Math.max(...times) - minTime || 1;
  const minValue = Math.min(...values);
  const valueSpan = Math.max(...values) - minValue || 1;
  const maxValue = Math.max(...values);
  // A label like "341,784,857" is wider than the default margin: widen the
  // margin to fit it, so no value is cut off at the chart's edge.
  const padLeft = Math.max(PAD_LEFT, Math.ceil(Math.max(format(maxValue).length, format(minValue).length) * TICK_CHAR_WIDTH) + 10);
  const x = (time: number) =>
    padLeft + ((time - minTime) / timeSpan) * (WIDTH - padLeft - PAD_RIGHT);
  const y = (value: number) =>
    HEIGHT - PAD_BOTTOM - ((value - minValue) / valueSpan) * (HEIGHT - PAD_TOP - PAD_BOTTOM);
  const scaleText =
    model.scale === "index"
      ? `Each line is its own value divided by its value in ${model.basePeriod}, times 100.`
      : `Published values, in ${unit || "the unit each row states"}.`;
  // Every period any line published, in time order (a label like "2020–2024"
  // sorts as text the same way, but time is the rule).
  const periodTimes = new Map<string, number>();
  for (const line of drawn) for (const point of line.points) if (!periodTimes.has(point.period)) periodTimes.set(point.period, point.time);
  const periods = [...periodTimes.keys()].sort((left, right) => periodTimes.get(left)! - periodTimes.get(right)!);
  const notPublishedThatYear = drawn.filter((line) => periods.some((period) => !line.points.some((point) => point.period === period)));
  const unpublished = model.lines.reduce((total, line) => total + line.unpublished, 0);

  return (
    <figure className="place-trend" data-testid={testId} data-scale={model.scale} data-base-period={model.basePeriod}>
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`${label}: ${drawn.length} line${drawn.length === 1 ? "" : "s"}, ${drawn.map((line) => line.place.name).join(", ")}`}
      >
        <line x1={padLeft} x2={WIDTH - PAD_RIGHT} y1={HEIGHT - PAD_BOTTOM} y2={HEIGHT - PAD_BOTTOM} className="place-trend-axis" />
        <text x={padLeft - 6} y={y(maxValue) + 4} textAnchor="end" className="place-trend-tick">{format(maxValue)}</text>
        <text x={padLeft - 6} y={y(minValue) + 4} textAnchor="end" className="place-trend-tick">{format(minValue)}</text>
        <text x={padLeft} y={HEIGHT - 10} className="place-trend-tick">{tick(periods[0])}</text>
        <text x={WIDTH - PAD_RIGHT} y={HEIGHT - 10} textAnchor="end" className="place-trend-tick">{tick(periods.at(-1))}</text>
        {hasBars
          ? drawn.map((line, index) => (
              <g key={`${line.place.geoId}-moe`} data-moe-series={line.place.geoId} stroke={STROKES[index % STROKES.length]} strokeWidth={1} opacity={0.7}>
                {line.points.filter((point) => point.moe !== null && point.moe > 0).map((point) => (
                  <g key={point.period} data-moe-period={point.period}>
                    <line x1={x(point.time)} x2={x(point.time)} y1={y(point.value - point.moe!)} y2={y(point.value + point.moe!)} />
                    <line x1={x(point.time) - 3} x2={x(point.time) + 3} y1={y(point.value - point.moe!)} y2={y(point.value - point.moe!)} />
                    <line x1={x(point.time) - 3} x2={x(point.time) + 3} y1={y(point.value + point.moe!)} y2={y(point.value + point.moe!)} />
                  </g>
                ))}
              </g>
            ))
          : null}
        {drawn.map((line, index) => (
          <polyline
            key={line.place.geoId}
            data-series={line.place.geoId}
            fill="none"
            stroke={STROKES[index % STROKES.length]}
            strokeDasharray={DASHES[index % DASHES.length] || undefined}
            strokeWidth={index === 0 ? 2.5 : 1.5}
            points={line.points.map((point) => `${x(point.time)},${y(point.value)}`).join(" ")}
          />
        ))}
      </svg>
      <figcaption>
        <ul className="place-trend-legend">
          {drawn.map((line, index) => (
            <li key={line.place.geoId}>
              <svg width="24" height="8" aria-hidden="true">
                <line x1="0" x2="24" y1="4" y2="4" stroke={STROKES[index % STROKES.length]} strokeWidth="2" strokeDasharray={DASHES[index % DASHES.length] || undefined} />
              </svg>
              {line.place.name}
              {index === 0 ? " (this page)" : " (reference)"}
            </li>
          ))}
        </ul>
        <p className="subtle" data-testid={`${testId}-scale`}>{scaleText}</p>
        {notes.filter((note) => hasBars || !note.startsWith("The bar")).map((note) => (
          <p className="subtle" key={note} data-testid={`${testId}-note`}>{note}</p>
        ))}
        {notPublishedThatYear.length ? (
          <p className="subtle" data-testid={`${testId}-not-published`}>
            A period with no point for {listNames(notPublishedThatYear.map((line) => line.place.name))} is one the source did not
            publish for that place: the line starts, ends or skips there, and nothing is filled in.
          </p>
        ) : null}
        {model.unindexed.length ? (
          <p className="subtle">Not drawn, because they publish no value for the base period: {model.unindexed.join(", ")}.</p>
        ) : null}
        {unpublished ? (
          <p className="subtle">{unpublished} published period{unpublished === 1 ? "" : "s"} without a number {unpublished === 1 ? "is" : "are"} not drawn.</p>
        ) : null}
      </figcaption>
      <details className="place-trend-values">
        <summary>Values drawn ({periods.length} period{periods.length === 1 ? "" : "s"})</summary>
        <div className="table-scroll">
        <table className="place-trend-table">
          <caption className="sr-only">{label}, values drawn</caption>
          <thead>
            <tr>
              <th scope="col">Period</th>
              {drawn.map((line) => <th scope="col" key={line.place.geoId}>{line.place.name}</th>)}
            </tr>
          </thead>
          <tbody>
            {periods.map((period) => (
              <tr key={period}>
                <th scope="row">{period}</th>
                {drawn.map((line) => {
                  const point = line.points.find((entry) => entry.period === period);
                  const margin = point?.moeText ? ` ${point.moeText}` : "";
                  return (
                    <td key={line.place.geoId}>
                      {point
                        ? model.scale === "index"
                          ? `${format(point.value)} (${format(point.published)}${margin})`
                          : `${format(point.value)}${margin}`
                        : line.unpublishedPeriods.includes(period)
                          ? "Published without a number"
                          : "Not published that year"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
        </div>
      </details>
    </figure>
  );
}
