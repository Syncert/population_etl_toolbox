"use client";

// Where a place's age structure differs from the selected comparison: per
// band, the place's share minus the comparison's, in percentage points, men
// and women as two thin bars either side of a zero line. Both shares are the
// API's; the subtraction is the only arithmetic here. A band either side
// does not publish is said, not drawn.

import { useState } from "react";

import { SEX_COLOURS } from "./AgePyramid";
import { SEXES, bandDifferences, sexWord, signedPoints } from "../lib/ageStructure";
import type { AgePlace, BandDifference } from "../lib/ageStructure";

const WIDTH = 680;
const ROW = 22;
const TOP = 10;
const BOTTOM = 30;
const LEFT = 92;
const RIGHT = 16;

function sentence(row: BandDifference, place: AgePlace, reference: AgePlace): string {
  const part = (value: number | null, sex: "men" | "women") =>
    value === null ? `${sexWord(sex)} not available` : `${sexWord(sex)} ${signedPoints(value)} points`;
  return `Age ${row.label}: ${part(row.men, "men")}, ${part(row.women, "women")}, ${place.geo_name} minus ${reference.geo_name}.`;
}

export default function AgeDifferenceBars({ place, reference, testId = "age-difference" }: { place: AgePlace; reference: AgePlace; testId?: string }) {
  const [focus, setFocus] = useState<number | null>(null);
  const rows = bandDifferences(place, reference);
  const n = rows.length;
  const height = TOP + ROW * n + BOTTOM;
  const widest = Math.max(0.5, ...rows.flatMap((row) => [row.men, row.women]).filter((v): v is number => v !== null).map(Math.abs));
  const limit = Math.ceil(widest * 2) / 2;
  const plotWidth = WIDTH - LEFT - RIGHT;
  const zero = LEFT + plotWidth / 2;
  const scale = (value: number) => (value / limit) * (plotWidth / 2);
  const step = limit <= 1.5 ? 0.5 : limit <= 4 ? 1 : 2;
  const ticks: number[] = [];
  for (let tick = -limit; tick <= limit + 1e-9; tick += step) ticks.push(Number(tick.toFixed(2)));
  const rowTop = (index: number) => TOP + ROW * (n - 1 - index);
  const focused = focus === null ? null : rows[focus];

  return (
    <figure className="age-difference" data-testid={testId} data-reference={reference.geo_id}>
      <ul className="chart-legend">
        {SEXES.map((sex) => (
          <li key={sex}>
            <span aria-hidden="true" className="legend-swatch" style={{ backgroundColor: SEX_COLOURS[sex] }} />
            <span>{sexWord(sex, true)}</span>
          </li>
        ))}
      </ul>
      <div className="age-chart-frame">
        <svg
          viewBox={`0 0 ${WIDTH} ${height}`}
          role="group"
          aria-label={`Difference in each age group's share, ${place.geo_name} minus ${reference.geo_name}, in percentage points. Use Tab to move through the age groups.`}
        >
          {ticks.map((tick) => (
            <g key={tick} aria-hidden="true">
              <line x1={zero + scale(tick)} x2={zero + scale(tick)} y1={TOP} y2={TOP + ROW * n} stroke={tick === 0 ? "#5b6872" : "#d6ddd9"} strokeWidth={tick === 0 ? 1.5 : 1} />
              <text x={zero + scale(tick)} y={height - 10} textAnchor="middle" fontSize="11" fill="#5b6872">{signedPoints(tick, step < 1 ? 1 : 0)}</text>
            </g>
          ))}
          {rows.map((row, index) => {
            const y = rowTop(index);
            return (
              <g key={row.bandId} data-testid={`${testId}-band-${row.bandId}`} data-men={row.men ?? ""} data-women={row.women ?? ""}>
                <text x={LEFT - 10} y={y + ROW / 2 + 4} textAnchor="end" fontSize="11" fill="#17232d" aria-hidden="true">{row.label}</text>
                {SEXES.map((sex, offset) => {
                  const value = row[sex];
                  if (value === null) {
                    return (
                      <text key={sex} x={zero + 6} y={y + 4 + offset * 9 + 7} fontSize="9" fontStyle="italic" fill="#5b6872" aria-hidden="true">not available</text>
                    );
                  }
                  const width = Math.abs(scale(value));
                  const x = value >= 0 ? zero : zero - width;
                  const barY = y + 3 + offset * 9;
                  const r = Math.min(3, width / 2);
                  const d =
                    width <= 0
                      ? ""
                      : value >= 0
                        ? `M${x},${barY} H${x + width - r} Q${x + width},${barY} ${x + width},${barY + r} V${barY + 7 - r} Q${x + width},${barY + 7} ${x + width - r},${barY + 7} H${x} Z`
                        : `M${x + width},${barY} H${x + r} Q${x},${barY} ${x},${barY + r} V${barY + 7 - r} Q${x},${barY + 7} ${x + r},${barY + 7} H${x + width} Z`;
                  return <path key={sex} d={d} fill={SEX_COLOURS[sex]} aria-hidden="true" />;
                })}
                <rect
                  className="age-bar-hit"
                  x={LEFT}
                  y={y}
                  width={plotWidth}
                  height={ROW}
                  fill="transparent"
                  tabIndex={0}
                  role="img"
                  aria-label={sentence(row, place, reference)}
                  onMouseEnter={() => setFocus(index)}
                  onMouseLeave={() => setFocus(null)}
                  onFocus={() => setFocus(index)}
                  onBlur={() => setFocus(null)}
                />
              </g>
            );
          })}
        </svg>
        {focused && focus !== null ? (
          <div className="age-tooltip" aria-hidden="true" data-testid={`${testId}-tooltip`} style={{ top: `${(rowTop(focus) / height) * 100}%`, left: `${((zero + 24) / WIDTH) * 100}%` }}>
            <strong>{`Age ${focused.label}`}</strong>
            {SEXES.map((sex) => (
              <span key={sex}>{`${sexWord(sex, true)}: ${focused[sex] === null ? "not available" : `${signedPoints(focused[sex])} points`}`}</span>
            ))}
          </div>
        ) : null}
      </div>
      <figcaption className="subtle">
        Percentage points: {place.geo_name}&apos;s share of each age group minus {reference.geo_name}&apos;s. Right of the line means
        {" "}{place.geo_name} has relatively more people that age. Both shares carry margins of error; a difference smaller than
        {" "}they are may be sampling.
      </figcaption>
    </figure>
  );
}
