import { formatObservationValue } from "../lib/explorerViewModel";

export default function TimeSeriesChart({ items }) {
  const series = (items || [])
    .map((item) => ({ ...item, numericValue: Number(item.value) }))
    .filter((item) => Number.isFinite(item.numericValue))
    .sort((left, right) => String(left.observation_date).localeCompare(String(right.observation_date)));

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
  const points = series.map((item, index) => {
    const x = series.length === 1
      ? width / 2
      : paddingX + (index / (series.length - 1)) * chartWidth;
    const y = paddingTop + ((maxValue - item.numericValue) / valueSpan) * chartHeight;
    return { ...item, x, y };
  });

  return (
    <div className="timeseries-chart">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        role="img"
        aria-label={`${series.length} time-series observations from ${series[0].observation_date} to ${series[series.length - 1].observation_date}`}
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
    </div>
  );
}
