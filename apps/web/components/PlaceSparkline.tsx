// A headline card's own history, drawn small (web-card-sparkline). Loaded
// with its own chunk so the place-page bundle carries only the import.

import type { ObservationRow } from "../lib/explorerViewModel";
import { sparkline } from "../lib/sparkline";

const WIDTH = 240;
const HEIGHT = 40;

export default function PlaceSparkline({ rows, testId, name }: { rows: ObservationRow[]; testId: string; name: string }) {
  const spark = sparkline(rows, WIDTH, HEIGHT);
  if (!spark) return null;
  const line = spark.points.map((point, index) => `${index ? "L" : "M"}${point.x.toFixed(1)} ${point.y.toFixed(1)}`).join(" ");
  const end = spark.points.at(-1)!;
  return (
    <figure className="place-spark" data-testid={testId} data-points={spark.points.length}>
      <svg viewBox={`-4 -4 ${WIDTH + 8} ${HEIGHT + 8}`} role="img" aria-label={`${name}, ${spark.first} to ${spark.last}`}>
        <path d={`${line} L${end.x.toFixed(1)} ${HEIGHT} L0 ${HEIGHT} Z`} className="place-spark-area" />
        <path d={line} className="place-spark-line" />
        <circle cx={end.x} cy={end.y} r={3.5} className="place-spark-end" />
      </svg>
      <figcaption>
        <span className="place-spark-ends">
          <span>{spark.first}</span>
          <span>{spark.last}</span>
        </span>
        {spark.change ? <span data-testid={`${testId}-change`}>{spark.change}</span> : null}
      </figcaption>
    </figure>
  );
}
