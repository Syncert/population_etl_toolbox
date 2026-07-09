export default function MiniLineChart({ items, label = "Time series" }) {
  const points = (items || [])
    .map((item) => ({ date: item.period || item.observation_date, value: Number(item.value) }))
    .filter((item) => item.date && Number.isFinite(item.value))
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));

  if (points.length < 2) {
    return <div className="empty-state compact">Not enough history is available to draw a trend.</div>;
  }

  const width = 680;
  const height = 230;
  const padding = { top: 18, right: 18, bottom: 34, left: 56 };
  const values = points.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const x = (index) => padding.left + (index / (points.length - 1)) * (width - padding.left - padding.right);
  const y = (value) => padding.top + ((max - value) / range) * (height - padding.top - padding.bottom);
  const path = points.map((point, index) => `${index ? "L" : "M"} ${x(index)} ${y(point.value)}`).join(" ");

  return (
    <div className="line-chart" role="img" aria-label={`${label}, ${points[0].date} to ${points.at(-1).date}`}>
      <svg viewBox={`0 0 ${width} ${height}`}>
        {[0, 0.5, 1].map((ratio) => {
          const value = max - ratio * range;
          const lineY = y(value);
          return <g key={ratio}><line x1={padding.left} x2={width - padding.right} y1={lineY} y2={lineY} /><text x={padding.left - 8} y={lineY + 4}>{value.toLocaleString(undefined, { maximumFractionDigits: 1 })}</text></g>;
        })}
        <path d={path} />
        <circle cx={x(points.length - 1)} cy={y(points.at(-1).value)} r="4" />
        <text className="x-label" x={padding.left} y={height - 8}>{points[0].date}</text>
        <text className="x-label end" x={width - padding.right} y={height - 8}>{points.at(-1).date}</text>
      </svg>
    </div>
  );
}
