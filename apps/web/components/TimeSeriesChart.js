import { formatObservationValue, publishedNumber } from "../lib/explorerViewModel";

/**
 * Whether the plotted points leave an interval materially wider than the rest.
 *
 * Only asked of a source that serves no value state, where a period without a
 * published number is absent from the series rather than present and marked
 * (API-127): the row the counted note below reads does not exist, so the only
 * evidence of the gap is the spacing. Those sources publish regular
 * histories -- monthly or annual -- so the median interval is the series' own
 * cadence, and a gap is an interval half again as long. Not twice as long:
 * calendar months are 28 to 31 days, so one skipped month is 1.94x a 31-day
 * neighbour and would slip under a doubling threshold, while February beside
 * January is 1.11x and stays well under this one. A series of two points has
 * one interval and therefore no cadence to compare it against.
 */
function hasAnIntervalWiderThanTheCadence(times) {
  if (times.length < 3) {
    return false;
  }
  const gaps = [];
  for (let index = 1; index < times.length; index += 1) {
    gaps.push(times[index] - times[index - 1]);
  }
  const ordered = [...gaps].sort((left, right) => left - right);
  const median = ordered[Math.floor(ordered.length / 2)];
  return median > 0 && Math.max(...gaps) >= median * 1.5;
}

export default function TimeSeriesChart({ items, publishesValueStatus = true }) {
  const rows = items || [];
  // `Number(null)` and `Number("")` are both 0, and 0 is finite -- so a
  // period the source suppressed or never published would join the line at
  // zero and the trend would describe a different series than the one the
  // source published. Reject the absent value before coercing.
  const series = rows
    .map((item) => ({ ...item, numericValue: publishedNumber(item.value) }))
    .filter((item) => item.numericValue !== null)
    .sort((left, right) => String(left.observation_date).localeCompare(String(right.observation_date)));
  const unpublished = rows.length - series.length;

  if (series.length === 0) {
    return <p className="subtle chart-empty">No time-series observations are available.</p>;
  }

  const width = 640;
  const height = 190;
  const paddingX = 26;
  const paddingTop = 18;
  const paddingBottom = 34;
  const values = series.map((item) => item.numericValue);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const valueSpan = maxValue - minValue || 1;
  const chartWidth = width - paddingX * 2;
  const chartHeight = height - paddingTop - paddingBottom;

  // The horizontal axis is time, not position in the list.
  //
  // Placing points by index closes the very gap this component was careful
  // not to fill with a zero: a series missing 1980 drew 1979 and 1981
  // adjacent and evenly spaced, and the line between them sloped as though
  // the measure had moved over one ordinary interval. The API leaves such a
  // gap open on purpose -- the consumer guide spends a paragraph on the July
  // 1980 county estimate the Bureau never published -- so the chart must
  // leave it open too (WEB-042).
  //
  // A date this cannot read, or a series that spans one instant, falls back
  // to the even spacing used before: still drawn, never dropped.
  const times = series.map((item) => Date.parse(String(item.observation_date)));
  const datedSpan = times.every((time) => Number.isFinite(time))
    ? Math.max(...times) - Math.min(...times)
    : 0;
  const firstTime = datedSpan > 0 ? Math.min(...times) : 0;

  const gapInTheCadence =
    datedSpan > 0 && hasAnIntervalWiderThanTheCadence(times);

  const points = series.map((item, index) => {
    const position = datedSpan > 0
      ? (times[index] - firstTime) / datedSpan
      : index / (series.length - 1 || 1);
    const x = series.length === 1 ? width / 2 : paddingX + position * chartWidth;
    const y = paddingTop + ((maxValue - item.numericValue) / valueSpan) * chartHeight;
    return { ...item, x, y };
  });

  return (
    <div className="timeseries-chart">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${series.length} time-series observation${series.length === 1 ? "" : "s"} from ${series[0].observation_date} to ${series[series.length - 1].observation_date}`}
      >
        <line className="chart-gridline" x1={paddingX} x2={width - paddingX} y1={paddingTop} y2={paddingTop} />
        <line className="chart-gridline" x1={paddingX} x2={width - paddingX} y1={paddingTop + chartHeight} y2={paddingTop + chartHeight} />
        {points.length > 1 ? (
          <polyline
            className="chart-line"
            points={points.map((point) => `${point.x},${point.y}`).join(" ")}
          />
        ) : null}
        {points.map((point) => (
          <circle key={`${point.observation_date}-${point.value}`} className="chart-point" cx={point.x} cy={point.y} r="4">
            <title>{`${point.observation_date}: ${formatObservationValue(point.numericValue)}`}</title>
          </circle>
        ))}
        <text className="chart-label" x={paddingX} y={height - 8}>{series[0].observation_date}</text>
        <text className="chart-label chart-label-end" x={width - paddingX} y={height - 8}>{series[series.length - 1].observation_date}</text>
        <text className="chart-value-label" x={paddingX} y={paddingTop - 5}>{formatObservationValue(maxValue)}</text>
        <text className="chart-value-label" x={paddingX} y={paddingTop + chartHeight - 5}>{formatObservationValue(minValue)}</text>
      </svg>
      {unpublished > 0 ? (
        <p className="subtle chart-note">
          {unpublished} period{unpublished === 1 ? "" : "s"} in this history published no value and
          {unpublished === 1 ? " is" : " are"} not plotted. An unpublished period is not a period
          with a value of zero.
        </p>
      ) : null}
      {/* The same fact for a source that never sends the row. Its serving
          relations carry only published numbers, so the count above is
          always zero and the gap shows only as spacing -- which reads as an
          interval the measure moved across rather than one it was not
          published for. */}
      {unpublished === 0 && !publishesValueStatus && gapInTheCadence ? (
        <p className="subtle chart-note" data-testid="chart-cadence-gap">
          This source serves only the periods it published a value for, so the
          wider span in this line is a period without a published value — not a
          period the measure moved across, and not a value of zero.
        </p>
      ) : null}
    </div>
  );
}
