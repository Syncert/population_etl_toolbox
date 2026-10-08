// One chapter's trend: this place as the primary line, its parents as
// reference lines.
//
// The rules are `TimeSeriesChart`'s: time is positioned by date, a period
// without a published number is not drawn and is counted, and every plotted
// value is readable in the table below the chart. The only transformation is
// `buildTrend`'s index for count measures, whose base period the caption
// states.

import type { TrendModel } from "../lib/placeChapters";
import { formatNumber } from "../lib/format";

const WIDTH = 640;
const HEIGHT = 220;
const PAD_LEFT = 52;
const PAD_RIGHT = 16;
const PAD_TOP = 14;
const PAD_BOTTOM = 34;

const STROKES = ["#0b6b57", "#8c3b17", "#2b4c8c"] as const;
const DASHES = ["", "6 4", "2 3"] as const;

function format(value: number): string {
  return formatNumber(value, { maximumFractionDigits: 2 });
}

export default function PlaceTrend({
  model,
  label,
  unit,
  testId,
}: {
  model: TrendModel;
  label: string;
  unit: string;
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
  const values = drawn.flatMap((line) => line.points.map((point) => point.value));
  const minTime = Math.min(...times);
  const timeSpan = Math.max(...times) - minTime || 1;
  const minValue = Math.min(...values);
  const valueSpan = Math.max(...values) - minValue || 1;
  const x = (time: number) =>
    PAD_LEFT + ((time - minTime) / timeSpan) * (WIDTH - PAD_LEFT - PAD_RIGHT);
  const y = (value: number) =>
    HEIGHT - PAD_BOTTOM - ((value - minValue) / valueSpan) * (HEIGHT - PAD_TOP - PAD_BOTTOM);
  const scaleText =
    model.scale === "index"
      ? `Each line is its own value divided by its value in ${model.basePeriod}, times 100.`
      : `Published values, in ${unit || "the unit each row states"}.`;
  const periods = [...new Set(drawn.flatMap((line) => line.points.map((point) => point.period)))].sort();
  const unpublished = model.lines.reduce((total, line) => total + line.unpublished, 0);

  return (
    <figure className="place-trend" data-testid={testId} data-scale={model.scale} data-base-period={model.basePeriod}>
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        role="img"
        aria-label={`${label}: ${drawn.length} line${drawn.length === 1 ? "" : "s"}, ${drawn.map((line) => line.place.name).join(", ")}`}
      >
        <line x1={PAD_LEFT} x2={WIDTH - PAD_RIGHT} y1={HEIGHT - PAD_BOTTOM} y2={HEIGHT - PAD_BOTTOM} className="place-trend-axis" />
        <text x={PAD_LEFT - 6} y={y(Math.max(...values)) + 4} textAnchor="end" className="place-trend-tick">{format(Math.max(...values))}</text>
        <text x={PAD_LEFT - 6} y={y(minValue) + 4} textAnchor="end" className="place-trend-tick">{format(minValue)}</text>
        <text x={PAD_LEFT} y={HEIGHT - 10} className="place-trend-tick">{drawn[0]!.points[0]?.period.slice(0, 4)}</text>
        <text x={WIDTH - PAD_RIGHT} y={HEIGHT - 10} textAnchor="end" className="place-trend-tick">{periods.at(-1)?.slice(0, 4)}</text>
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
                  return (
                    <td key={line.place.geoId}>
                      {point ? (model.scale === "index" ? `${format(point.value)} (${format(point.published)})` : format(point.value)) : "Not published"}
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
