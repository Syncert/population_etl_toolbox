"use client";

// The broad age groups across the survey periods `/age-structure/history`
// returned (non-overlapping five-year periods by default): one small chart
// per group, each on its own scale and labelled with its values, because a
// group of 2 % and a group of 65 % on one axis would flatten the first. A
// period the place did not publish is a gap in the line, never zero.

import { BROAD_GROUPS, marginText, num, percentText, periodText } from "../lib/ageStructure";
import type { AgeHistoryResponse, AgePlace } from "../lib/ageStructure";

const WIDTH = 220;
const HEIGHT = 120;
const PAD_X = 28;
const PAD_TOP = 22;
const PAD_BOTTOM = 26;
const INK = "#0b6b57";

type GroupKey = (typeof BROAD_GROUPS)[number]["key"];

function shareOf(item: AgePlace, key: GroupKey): number | null {
  return num(item.indicators?.[key]?.share ?? null);
}

export default function AgeStructureHistory({ history, testId = "age-history" }: { history: AgeHistoryResponse; testId?: string }) {
  // Oldest on the left.
  const items = history.items.slice().reverse();
  if (items.length < 2) {
    return (
      <p className="subtle" data-testid={`${testId}-empty`}>
        Only one survey period is published for this place, so there is no change to draw.
      </p>
    );
  }
  const x = (index: number) => PAD_X + (index / (items.length - 1)) * (WIDTH - PAD_X * 2);
  return (
    <div className="age-history" data-testid={testId} data-periods={items.map((item) => item.release).join(",")}>
      <div className="age-history-grid">
        {BROAD_GROUPS.map((group) => {
          const values = items.map((item) => shareOf(item, group.key));
          const present = values.filter((value): value is number => value !== null);
          const lo = present.length ? Math.min(...present) : 0;
          const hi = present.length ? Math.max(...present) : 1;
          const pad = Math.max(0.5, (hi - lo) * 0.25);
          const min = Math.max(0, lo - pad);
          const max = hi + pad;
          const y = (value: number) => PAD_TOP + ((max - value) / (max - min || 1)) * (HEIGHT - PAD_TOP - PAD_BOTTOM);
          let d = "";
          let open = false;
          values.forEach((value, index) => {
            if (value === null) {
              open = false;
              return;
            }
            d += `${open ? " L" : " M"}${x(index)},${y(value)}`;
            open = true;
          });
          const summary = items
            .map((item, index) => `${periodText(item)}: ${values[index] === null ? "not published" : percentText(values[index])}`)
            .join("; ");
          return (
            <figure key={group.key} className="age-history-chart" data-testid={`${testId}-${group.key}`}>
              <figcaption>{group.label}</figcaption>
              <svg viewBox={`0 0 ${WIDTH} ${HEIGHT}`} role="img" aria-label={`${group.label}, share of everyone: ${summary}.`}>
                <line x1={PAD_X} x2={WIDTH - PAD_X} y1={HEIGHT - PAD_BOTTOM} y2={HEIGHT - PAD_BOTTOM} stroke="#d6ddd9" />
                <path d={d.trim()} fill="none" stroke={INK} strokeWidth="2" strokeLinejoin="round" />
                {values.map((value, index) =>
                  value === null ? null : (
                    <g key={index}>
                      <circle cx={x(index)} cy={y(value)} r="4" fill={INK} stroke="#ffffff" strokeWidth="2" />
                      <text x={x(index)} y={y(value) - 8} textAnchor="middle" fontSize="11" fill="#17232d">{percentText(value)}</text>
                    </g>
                  ),
                )}
                {items.map((item, index) => (
                  <text key={item.release || index} x={x(index)} y={HEIGHT - 8} textAnchor="middle" fontSize="10" fill="#5b6872">{periodText(item)}</text>
                ))}
              </svg>
            </figure>
          );
        })}
      </div>
      <div className="table-scroll">
        <table className="data-table" data-testid={`${testId}-table`}>
          <caption className="subtle">Share of everyone in each broad age group, by survey period, with 90 % margins of error.</caption>
          <thead>
            <tr>
              <th scope="col">Survey period</th>
              {BROAD_GROUPS.map((group) => <th scope="col" key={group.key}>{group.label}</th>)}
            </tr>
          </thead>
          <tbody>
            {history.items.map((item) => (
              <tr key={item.release || item.period_end || ""}>
                <th scope="row">{periodText(item)}</th>
                {BROAD_GROUPS.map((group) => {
                  const indicator = item.indicators?.[group.key];
                  return <td key={group.key}>{indicator ? `${percentText(indicator.share)} ${marginText(indicator.share_moe)}`.trim() : "not published"}</td>;
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
