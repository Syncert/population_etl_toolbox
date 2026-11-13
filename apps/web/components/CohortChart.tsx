"use client";

// Following an age group forward five years (ADR-0013 rule 6): per band, the
// count in the earlier survey beside the count one band up in the later one.
// It is a synthetic cohort, not a panel, and the API's caveat says so above
// the chart. A refused pair is said in words and nothing is drawn. Each
// sentence uses only ADR-0011's verbs: "rose", "fell", "was unchanged", and
// "was not distinguishable from" when the two 90 % intervals overlap.

import { SEX_COLOURS } from "./AgePyramid";
import { SEXES, bandIdText, cohortRefusalText, cohortSentence, countText, num, periodText, sexWord } from "../lib/ageStructure";
import type { AgeCohortsResponse } from "../lib/ageStructure";

const WIDTH = 680;
const ROW = 26;
const LEFT = 150;
const RIGHT = 16;
const TOP = 8;
const BOTTOM = 8;

export default function CohortChart({ cohorts, testId = "age-cohorts" }: { cohorts: AgeCohortsResponse; testId?: string }) {
  if (!cohorts.comparable || cohorts.refusal || !cohorts.from || !cohorts.to) {
    return (
      <p className="notice" data-testid={`${testId}-refusal`} data-reason={cohorts.refusal?.reason || ""}>
        {cohorts.refusal ? cohortRefusalText(cohorts.refusal) : "These two surveys cannot be compared for this place."}
      </p>
    );
  }
  const fromPeriod = cohorts.from;
  const toPeriod = cohorts.to;
  const from = periodText(fromPeriod);
  const to = periodText(toPeriod);
  const items = cohorts.items;
  const widest = Math.max(1, ...items.flatMap((item) => SEXES.flatMap((sex) => [num(item[sex].from_count), num(item[sex].to_count)])).filter((v): v is number => v !== null));
  const plot = WIDTH - LEFT - RIGHT;
  const scale = (value: number) => (value / widest) * plot;
  return (
    <div className="age-cohorts" data-testid={testId}>
      <p className="notice" data-testid={`${testId}-caveat`}>{cohorts.caveat}</p>
      {SEXES.map((sex) => {
        const height = TOP + ROW * items.length + BOTTOM;
        return (
          <figure key={sex} data-testid={`${testId}-${sex}`}>
            <figcaption>
              <strong>{sexWord(sex, true)}</strong>
              <span className="subtle">{` · lighter bar ${from}, darker bar the same people's age group five years on, ${to}`}</span>
            </figcaption>
            <svg viewBox={`0 0 ${WIDTH} ${height}`} role="group" aria-label={`${sexWord(sex, true)}: each age group in ${from} beside the next age group in ${to}.`}>
              {items.map((item, index) => {
                const y = TOP + ROW * index;
                const before = num(item[sex].from_count);
                const after = num(item[sex].to_count);
                return (
                  <g key={item.from_band} data-testid={`${testId}-${sex}-${item.from_band}`}>
                    <text x={LEFT - 8} y={y + ROW / 2 + 4} textAnchor="end" fontSize="11" fill="#17232d" aria-hidden="true">
                      {`${bandIdText(item.from_band)} → ${bandIdText(item.to_band)}`}
                    </text>
                    {before === null ? null : <rect x={LEFT} y={y + 3} width={Math.max(0.5, scale(before))} height={9} rx="2" fill={SEX_COLOURS[sex]} fillOpacity="0.45" aria-hidden="true" />}
                    {after === null ? null : <rect x={LEFT} y={y + 14} width={Math.max(0.5, scale(after))} height={9} rx="2" fill={SEX_COLOURS[sex]} aria-hidden="true" />}
                    <rect
                      className="age-bar-hit"
                      x={0}
                      y={y}
                      width={WIDTH}
                      height={ROW}
                      fill="transparent"
                      tabIndex={0}
                      role="img"
                      aria-label={cohortSentence(item, sex, fromPeriod, toPeriod)}
                    >
                      <title>{cohortSentence(item, sex, fromPeriod, toPeriod)}</title>
                    </rect>
                  </g>
                );
              })}
            </svg>
          </figure>
        );
      })}
      <div className="table-scroll">
        <table className="data-table" data-testid={`${testId}-table`}>
          <caption className="subtle">{`Counts in ${from} and, five years on, ${to}, with the change and its 90 % margin of error.`}</caption>
          <thead>
            <tr>
              <th scope="col">Age group then → now</th>
              {SEXES.map((sex) => <th scope="col" key={sex}>{sexWord(sex, true)}</th>)}
            </tr>
          </thead>
          <tbody>
            {items.map((item) => (
              <tr key={item.from_band}>
                <th scope="row">{`${bandIdText(item.from_band)} → ${bandIdText(item.to_band)}`}</th>
                {SEXES.map((sex) => (
                  <td key={sex} data-testid={`${testId}-sentence-${sex}-${item.from_band}`}>
                    {cohortSentence(item, sex, fromPeriod, toPeriod)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="subtle">{`${countText(String(items.length))} age groups move up one band; the open-ended 85 and over group has no next band.`}</p>
    </div>
  );
}
