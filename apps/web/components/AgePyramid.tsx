"use client";

// A population pyramid (web-age-and-sex-structure, ADR-0013), hand-drawn in
// SVG like BarChart: eighteen five-year bands, youngest at the bottom, men to
// the left and women to the right of a centre gap that carries the band
// labels. Each bar is the band's share of everyone in the place (both
// sexes), so a county and its state sit on one scale.
//
// - Whiskers are the API's 90 % margins on the share.
// - A band the API marks unreliable (coefficient of variation over 30 %) is
//   hatched and says so in its label and tooltip.
// - A band with no value is drawn absent with its reason, never as a bar of
//   length zero: zero is a count a tract can really have.
// - The selected comparison is a dark outline on the same scale; an earlier
//   release of the same place is a dashed outline.
// - Every bar takes keyboard focus and announces band, sex, share, count and
//   margin; the table view carries the same numbers.

import { useId, useState } from "react";

import { seriesStroke } from "./LineChart";
import {
  SEXES,
  bandSentence,
  bandText,
  countText,
  num,
  percentText,
  periodText,
  pyramidExtent,
  sexWord,
  unavailableText,
} from "../lib/ageStructure";
import type { AgeBand, AgePlace, Sex } from "../lib/ageStructure";

/** The site's first two categorical slots: men, then women. */
export const SEX_COLOURS: Record<Sex, string> = { men: seriesStroke(0), women: seriesStroke(1) };

const WIDTH = 680;
const ROW = 22;
const TOP = 30;
const BOTTOM = 34;
const GAP = 84;
const SIDE = 18;
const REFERENCE_INK = "#17232d";
const EARLIER_INK = "#5b6872";

function barPath(x: number, y: number, w: number, h: number, side: "left" | "right"): string {
  if (w <= 0) return "";
  const r = Math.min(4, w / 2, h / 2);
  if (side === "left") {
    return `M${x + w},${y} H${x + r} Q${x},${y} ${x},${y + r} V${y + h - r} Q${x},${y + h} ${x + r},${y + h} H${x + w} Z`;
  }
  return `M${x},${y} H${x + w - r} Q${x + w},${y} ${x + w},${y + r} V${y + h - r} Q${x + w},${y + h} ${x + w - r},${y + h} H${x} Z`;
}

/** The stepped outline of one side of a place's pyramid; a band with no share breaks the line. */
function outlinePath(place: AgePlace, sex: Sex, x: (share: number, sex: Sex) => number, rowTop: (index: number) => number): string {
  let path = "";
  let open = false;
  place.bands.forEach((band, index) => {
    const share = num(band[sex].share);
    if (share === null) {
      open = false;
      return;
    }
    const px = x(share, sex);
    const bottom = rowTop(index) + ROW;
    const top = rowTop(index);
    path += `${open ? " L" : " M"}${px},${bottom} L${px},${top}`;
    open = true;
  });
  return path.trim();
}

export interface AgePyramidProps {
  place: AgePlace;
  reference?: AgePlace | null;
  earlier?: AgePlace | null;
  /** Shared with other pyramids drawn beside this one (the compare page). */
  extent?: number;
  testId?: string;
}

export default function AgePyramid({ place, reference = null, earlier = null, extent, testId = "age-pyramid" }: AgePyramidProps) {
  const patternId = `age-hatch-${useId().replace(/:/g, "")}`;
  const [focus, setFocus] = useState<{ index: number; sex: Sex } | null>(null);
  const bands = place.bands;
  const n = bands.length;
  const height = TOP + ROW * n + BOTTOM;
  const half = (WIDTH - GAP) / 2;
  const reach = half - SIDE;
  const max = extent ?? pyramidExtent([place, reference, earlier]);
  const scale = (share: number) => (share / max) * reach;
  const leftEdge = half;
  const rightEdge = half + GAP;
  const x = (share: number, sex: Sex) => (sex === "men" ? leftEdge - scale(share) : rightEdge + scale(share));
  // Youngest at the bottom.
  const rowTop = (index: number) => TOP + ROW * (n - 1 - index);
  const ticks = Array.from({ length: max + 1 }, (_, tick) => tick).filter((tick) => max <= 6 || tick % 2 === 0);
  const others = [reference, earlier].filter((item): item is AgePlace => Boolean(item));
  const focused = focus ? bands[focus.index] : null;
  const label =
    `Population pyramid of ${place.geo_name}, ${periodText(place)}: men on the left and women on the right, ` +
    `each age group as a percent of everyone who lives there` +
    (reference ? `, with ${reference.geo_name} outlined` : "") +
    (earlier ? ` and ${periodText(earlier)} dashed` : "") +
    ". Use Tab to move through the bars.";

  return (
    <figure className="age-pyramid" data-testid={testId} data-geo-id={place.geo_id} data-reference={reference?.geo_id || "none"} data-earlier={earlier?.release || ""}>
      <ul className="chart-legend" data-testid={`${testId}-legend`}>
        {SEXES.map((sex) => (
          <li key={sex}>
            <span aria-hidden="true" className="legend-swatch" style={{ backgroundColor: SEX_COLOURS[sex] }} />
            <span>{`${sexWord(sex, true)}, ${place.geo_name}`}</span>
          </li>
        ))}
        {reference ? (
          <li data-testid={`${testId}-legend-reference`}>
            <span aria-hidden="true" className="legend-line" style={{ borderTopColor: REFERENCE_INK }} />
            <span>{`${reference.geo_name}, ${periodText(reference)}`}</span>
          </li>
        ) : null}
        {earlier ? (
          <li data-testid={`${testId}-legend-earlier`}>
            <span aria-hidden="true" className="legend-line legend-line-dashed" style={{ borderTopColor: EARLIER_INK }} />
            <span>{`${place.geo_name}, ${periodText(earlier)}`}</span>
          </li>
        ) : null}
        {bands.some((band) => SEXES.some((sex) => num(band[sex].share) !== null && !band[sex].reliable)) ? (
          <li data-testid={`${testId}-legend-unreliable`}>
            <svg aria-hidden="true" width="14" height="12" className="legend-hatch"><rect width="14" height="12" fill={`url(#${patternId})`} stroke="#5b6872" /></svg>
            <span>Hatched: the survey&apos;s estimate is unreliable</span>
          </li>
        ) : null}
      </ul>
      <div className="age-chart-frame">
        <svg viewBox={`0 0 ${WIDTH} ${height}`} role="group" aria-label={label} data-testid={`${testId}-svg`}>
          <defs>
            <pattern id={patternId} width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
              <rect width="6" height="6" fill="#ffffff" />
              <line x1="0" y1="0" x2="0" y2="6" stroke="#5b6872" strokeWidth="2.5" />
            </pattern>
          </defs>
          {ticks.map((tick) => (
            <g key={tick} aria-hidden="true">
              {[leftEdge - scale(tick), rightEdge + scale(tick)].map((px, side) => (
                <g key={side}>
                  <line className="chart-gridline" x1={px} x2={px} y1={TOP - 4} y2={TOP + ROW * n} stroke="#d6ddd9" strokeWidth="1" />
                  <text x={px} y={height - BOTTOM + 18} textAnchor="middle" fontSize="11" fill="#5b6872">{`${tick}%`}</text>
                </g>
              ))}
            </g>
          ))}
          <text x={leftEdge - 4} y={16} textAnchor="end" fontSize="13" fontWeight="600" fill="#17232d" aria-hidden="true">Men</text>
          <text x={rightEdge + 4} y={16} fontSize="13" fontWeight="600" fill="#17232d" aria-hidden="true">Women</text>
          {bands.map((band, index) => {
            const y = rowTop(index);
            const barY = y + 3;
            const barH = ROW - 6;
            return (
              <g key={band.band_id} data-testid={`${testId}-band-${band.band_id}`}>
                <text x={WIDTH / 2} y={y + ROW / 2 + 4} textAnchor="middle" fontSize="11" fill="#17232d" aria-hidden="true">{bandText(band)}</text>
                {SEXES.map((sex) => (
                  <Bar
                    key={sex}
                    band={band}
                    sex={sex}
                    barY={barY}
                    barH={barH}
                    edge={sex === "men" ? leftEdge : rightEdge}
                    scale={scale}
                    patternId={patternId}
                    testId={`${testId}-bar-${band.band_id}-${sex}`}
                  />
                ))}
              </g>
            );
          })}
          {earlier
            ? SEXES.map((sex) => (
                <path key={`earlier-${sex}`} d={outlinePath(earlier, sex, x, rowTop)} fill="none" stroke={EARLIER_INK} strokeWidth="2" strokeDasharray="5 4" strokeLinejoin="round" data-testid={`${testId}-earlier-${sex}`} aria-hidden="true" />
              ))
            : null}
          {reference
            ? SEXES.map((sex) => {
                const d = outlinePath(reference, sex, x, rowTop);
                return (
                  <g key={`reference-${sex}`} aria-hidden="true">
                    <path d={d} fill="none" stroke="#ffffff" strokeWidth="4.5" strokeLinejoin="round" />
                    <path d={d} fill="none" stroke={REFERENCE_INK} strokeWidth="2" strokeLinejoin="round" data-testid={`${testId}-reference-${sex}`} />
                  </g>
                );
              })
            : null}
          {bands.map((band, index) =>
            SEXES.map((sex) => (
              <rect
                key={`${band.band_id}-${sex}-hit`}
                className="age-bar-hit"
                x={sex === "men" ? 0 : rightEdge}
                y={rowTop(index)}
                width={sex === "men" ? leftEdge : WIDTH - rightEdge}
                height={ROW}
                fill="transparent"
                tabIndex={0}
                role="img"
                aria-label={bandSentence(place, band, sex, others)}
                data-testid={`${testId}-hit-${band.band_id}-${sex}`}
                onMouseEnter={() => setFocus({ index, sex })}
                onMouseLeave={() => setFocus(null)}
                onFocus={() => setFocus({ index, sex })}
                onBlur={() => setFocus(null)}
              />
            )),
          )}
        </svg>
        {focus && focused ? (
          <div
            className="age-tooltip"
            data-testid={`${testId}-tooltip`}
            aria-hidden="true"
            style={{
              top: `${(rowTop(focus.index) / height) * 100}%`,
              ...(focus.sex === "men" ? { left: `${(rightEdge / WIDTH) * 100}%` } : { right: `${((WIDTH - leftEdge) / WIDTH) * 100}%` }),
            }}
          >
            <Tooltip place={place} band={focused} sex={focus.sex} others={others} />
          </div>
        ) : null}
      </div>
    </figure>
  );
}

function Bar({
  band,
  sex,
  barY,
  barH,
  edge,
  scale,
  patternId,
  testId,
}: {
  band: AgeBand;
  sex: Sex;
  barY: number;
  barH: number;
  edge: number;
  scale: (share: number) => number;
  patternId: string;
  testId: string;
}) {
  const side = band[sex];
  const share = num(side.share);
  const left = sex === "men";
  if (share === null) {
    // Absent: a short word at the centre gap, never a zero-length bar.
    return (
      <text
        x={left ? edge - 6 : edge + 6}
        y={barY + barH / 2 + 4}
        textAnchor={left ? "end" : "start"}
        fontSize="11"
        fontStyle="italic"
        fill="#5b6872"
        data-testid={testId}
        data-state="absent"
        data-reason={side.unavailable_reason || ""}
        aria-hidden="true"
      >
        not available
      </text>
    );
  }
  const width = scale(share);
  const moe = num(side.share_moe);
  const x = left ? edge - width : edge;
  const mid = barY + barH / 2;
  const lo = moe === null ? null : Math.max(0, share - moe);
  const hi = moe === null ? null : share + moe;
  const px = (value: number) => (left ? edge - scale(value) : edge + scale(value));
  return (
    <g data-testid={testId} data-state={side.reliable ? "reliable" : "unreliable"} aria-hidden="true">
      <path d={barPath(x, barY, width, barH, left ? "left" : "right")} fill={SEX_COLOURS[sex]} />
      {side.reliable ? null : (
        <path d={barPath(x, barY, width, barH, left ? "left" : "right")} fill={`url(#${patternId})`} fillOpacity="0.85" stroke={SEX_COLOURS[sex]} strokeWidth="1" />
      )}
      {lo !== null && hi !== null ? (
        <g stroke="#17232d" strokeWidth="1.2" strokeOpacity="0.7">
          <line x1={px(lo)} x2={px(hi)} y1={mid} y2={mid} />
          <line x1={px(lo)} x2={px(lo)} y1={mid - 3} y2={mid + 3} />
          <line x1={px(hi)} x2={px(hi)} y1={mid - 3} y2={mid + 3} />
        </g>
      ) : null}
    </g>
  );
}

function Tooltip({ place, band, sex, others }: { place: AgePlace; band: AgeBand; sex: Sex; others: readonly AgePlace[] }) {
  const side = band[sex];
  const share = num(side.share);
  return (
    <>
      <strong>
        <span className="legend-swatch" style={{ backgroundColor: SEX_COLOURS[sex] }} aria-hidden="true" />
        {`${sexWord(sex, true)}, ${bandText(band)}`}
      </strong>
      {share === null ? (
        <span>{unavailableText(side.unavailable_reason)}</span>
      ) : (
        <>
          <span>{`Share of ${place.geo_name}: ${percentText(side.share)}`}</span>
          <span>{`People: ${countText(side.count)}${num(side.count_moe) === null ? "" : ` ± ${countText(side.count_moe)}`}`}</span>
          {side.reliable ? null : <span>Unreliable: the margin of error is large next to the count.</span>}
        </>
      )}
      {others.map((other) => {
        const match = other.bands.find((item) => item.band_id === band.band_id);
        return (
          <span key={`${other.geo_id}-${other.release}`}>
            {`${other.role === "place" ? `${other.geo_name}, ${periodText(other)}` : other.geo_name}: ${match ? percentText(match[sex].share) : "not available"}`}
          </span>
        );
      })}
    </>
  );
}

/** The same numbers as the chart, oldest band first, as the pyramid reads top to bottom. */
export function AgePyramidTable({ place, reference = null, testId = "age-pyramid-table" }: { place: AgePlace; reference?: AgePlace | null; testId?: string }) {
  const byId = new Map((reference?.bands || []).map((band) => [band.band_id, band]));
  return (
    <div className="table-scroll" data-testid={testId}>
      <table className="data-table age-table">
        <caption className="subtle">{`${place.geo_name}, ${periodText(place)}: people and share of everyone by age group, with 90 % margins of error.`}</caption>
        <thead>
          <tr>
            <th scope="col">Age</th>
            <th scope="col">Men</th>
            <th scope="col">± margin</th>
            <th scope="col">Men, share</th>
            <th scope="col">Women</th>
            <th scope="col">± margin</th>
            <th scope="col">Women, share</th>
            {reference ? <th scope="col">{`${reference.geo_name}, men`}</th> : null}
            {reference ? <th scope="col">{`${reference.geo_name}, women`}</th> : null}
          </tr>
        </thead>
        <tbody>
          {place.bands.slice().reverse().map((band) => {
            const other = byId.get(band.band_id);
            return (
              <tr key={band.band_id} data-band={band.band_id}>
                <th scope="row">{bandText(band)}</th>
                {SEXES.map((sex) => (
                  <Cells key={sex} band={band} sex={sex} />
                ))}
                {reference ? <td>{other ? percentText(other.men.share) : "not available"}</td> : null}
                {reference ? <td>{other ? percentText(other.women.share) : "not available"}</td> : null}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Cells({ band, sex }: { band: AgeBand; sex: Sex }) {
  const side = band[sex];
  if (num(side.share) === null) {
    return (
      <>
        <td colSpan={3} className="subtle">{unavailableText(side.unavailable_reason)}</td>
      </>
    );
  }
  const mark = side.reliable ? "" : " (unreliable)";
  return (
    <>
      <td>{countText(side.count)}</td>
      <td>{num(side.count_moe) === null ? "" : countText(side.count_moe)}</td>
      <td>{`${percentText(side.share)}${mark}`}</td>
    </>
  );
}
