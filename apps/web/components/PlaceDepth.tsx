// A chapter's depth rows and industry tables (web-compact-depth). Loaded as
// its own chunk, so the place-page bundle carries only the import.
//
// What the rows share -- source, period, what "±" means -- is said once above
// them; each row keeps its universe, its value and its margin as a share. A
// row whose source or period differs from the rest says so on the row.

import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue } from "../lib/explorerViewModel";
import { observationPeriodLabel } from "../lib/observationAccess";
import { measureBasis, publishedUnit, publishesAt, trendPeriodLabel } from "../lib/placeChapters";
import type { LevelAnswer, LevelPlace, ResolvedPlaceMeasure } from "../lib/placeChapters";
import { depthShared, gapReason, industryBars, marginShare, sharedLine, needsCare } from "../lib/placeDepth";
import type { IndustryEntry } from "../lib/placeDepth";
import { pairingFor } from "../lib/derivedViews";
import type { DerivedRatioObservation, DerivedRatioPairing, ViewChoice } from "../lib/derivedViews";
import { DerivedValue } from "./DerivedView";

const NOT_LOADED = "Not loaded into this site yet.";

interface DerivedAnswer {
  row: DerivedRatioObservation | null;
  error?: string;
  pending?: boolean;
}

const key = (left: string, right: string) => `${left}|${right}`;

function gapText(measure: ResolvedPlaceMeasure, place: LevelPlace, answer: LevelAnswer | undefined): string {
  if (!measure.metric) return NOT_LOADED;
  if (!publishesAt(measure, place.level)) return `Not published at ${place.level.toLowerCase()} grain`;
  if (answer?.error) return answer.error;
  if (answer?.pending) return "Reading…";
  return gapReason(answer?.row ?? null, "Not published for this place");
}

function published(row: ObservationRow | null | undefined): row is ObservationRow {
  return Boolean(row) && row!.value !== null && row!.value !== undefined && row!.value !== "";
}

export interface PlaceDepthProps {
  chapterId: string;
  measures: ResolvedPlaceMeasure[];
  place: LevelPlace;
  levels: LevelPlace[];
  answers: Map<string, LevelAnswer>;
  view: ViewChoice;
  pairings: DerivedRatioPairing[];
  derived: Map<string, DerivedAnswer>;
}

export default function PlaceDepth({ chapterId, measures, place, levels, answers, view, pairings, derived }: PlaceDepthProps) {
  const listed = measures.filter((measure) => !measure.measure.group);
  const rows = listed.map((measure) => ({
    measure,
    metricCode: measure.metricCode,
    answer: answers.get(key(measure.metricCode, place.geoId)),
    pairing: measure.metric ? pairingFor(pairings, measure.metricCode, view) : null,
  }));
  const shared = depthShared(rows.filter((entry) => !entry.pairing).map((entry) => ({ metricCode: entry.metricCode, row: entry.answer?.row ?? null })));
  const header = sharedLine(shared);
  const earnings = measures.filter((measure) => measure.measure.group === "bea-earnings" && measure.metric);
  const mix = measures.filter((measure) => measure.measure.group === "industry-mix" && measure.metric);
  return (
    <>
      {listed.length ? (
        <div className="place-depth-block">
          {header ? <p className="place-depth-shared" data-testid={`chapter-${chapterId}-depth-shared`}>{header}</p> : null}
          <dl className="place-depth" data-testid={`chapter-${chapterId}-depth`}>
            {rows.map(({ measure, answer, pairing }) => {
              if (pairing) {
                const shown = levels.filter((entry) => publishesAt(measure, entry.level));
                return (
                  <div key={measure.measure.id} data-testid={`depth-${measure.measure.id}`} data-view={view}>
                    <dt>
                      {pairing.label}
                      <span className="place-universe"> · Universe: {pairing.universe}</span>
                    </dt>
                    <dd>
                      <ul className="derived-levels">
                        {shown.map((entry) => (
                          <li key={entry.geoId} data-testid={`depth-${measure.measure.id}-${entry.level.toLowerCase()}`}>
                            <strong>{entry.name}:</strong>{" "}
                            <DerivedValue pairing={pairing} {...(derived.get(key(pairing.ratio_id, entry.geoId)) || { row: null, pending: true })} />
                          </li>
                        ))}
                      </ul>
                    </dd>
                  </div>
                );
              }
              const row = answer?.row ?? null;
              const value = published(row) && measure.metric && publishesAt(measure, place.level) && !answer?.error;
              const basis = measure.measure.basis || (measure.metric ? measureBasis(measure.metricCode) : null);
              const period = value ? trendPeriodLabel(row!) : "";
              const care = value && needsCare(row!);
              return (
                <div key={measure.measure.id} data-testid={`depth-${measure.measure.id}`} data-care={care ? "true" : undefined}>
                  <dt>
                    {measure.measure.label}
                    {measure.measure.universe ? <span className="place-universe"> · Universe: {measure.measure.universe}</span> : null}
                    {basis ? (
                      <span className={basis === shared.basis ? "sr-only" : "place-basis"} data-testid={`depth-${measure.measure.id}-basis`}> · {basis}</span>
                    ) : null}
                  </dt>
                  <dd>
                    {view !== "count" && measure.metric ? (
                      <span className="subtle" data-testid={`depth-${measure.measure.id}-count-only`}>
                        No reviewed {view === "share" ? "share" : "per-resident view"} for this count; showing the count.{" "}
                      </span>
                    ) : null}
                    {value ? (
                      <>
                        <span className="place-depth-value">{`${formatObservationValue(row!.value)} ${publishedUnit(row!, measure.measure.unit)}`.trim()}</span>
                        {marginShare(row!) ? <span className="subtle"> {marginShare(row!)}</span> : null}
                        {period !== shared.period ? <span className="subtle" title={observationPeriodLabel(row)}> · {period}</span> : null}
                        {care ? <span className="subtle" data-testid={`depth-${measure.measure.id}-care`}> · use with care</span> : null}
                      </>
                    ) : (
                      <span className="subtle">{gapText(measure, place, answer)}</span>
                    )}
                  </dd>
                </div>
              );
            })}
          </dl>
        </div>
      ) : null}
      <IndustryBars testId="bea-earnings" rows={earnings} place={place} answers={answers} heading={`Earnings from work located in ${place.name}, by industry`} unitHeading />
      <IndustryBars testId="industry-mix" rows={mix} place={place} answers={answers} heading={`Private jobs located in ${place.name}, by industry`} />
    </>
  );
}

/**
 * Each sector's own published figure for its newest period, drawn as a bar
 * from zero, largest first. Nothing sums the sectors or computes a share, so
 * a withheld sector reads as withheld rather than as a gap someone filled
 * with arithmetic (bea-regional-accounts, bls-qcew-county-wages).
 */
function IndustryBars({
  testId,
  rows,
  place,
  answers,
  heading,
  unitHeading,
}: {
  testId: string;
  rows: ResolvedPlaceMeasure[];
  place: LevelPlace;
  answers: Map<string, LevelAnswer>;
  heading: string;
  unitHeading?: boolean;
}) {
  if (!rows.length) return null;
  const read = rows.map((measure) => ({ measure, answer: answers.get(key(measure.metricCode, place.geoId)) }));
  const first = read.find((entry) => published(entry.answer?.row))?.answer?.row ?? null;
  const period = first ? trendPeriodLabel(first) : "";
  const unit = unitHeading && first ? publishedUnit(first) : "";
  const entries: IndustryEntry[] = read.map(({ measure, answer }) => {
    const row = answer?.row ?? null;
    const ok = published(row) && publishesAt(measure, place.level) && !answer?.error && !answer?.pending;
    const number = ok ? Number(row!.value) : NaN;
    return {
      id: measure.measure.id,
      label: measure.measure.label,
      value: Number.isFinite(number) ? number : null,
      message: gapText(measure, place, answer),
    };
  });
  const { bars, max, gaps } = industryBars(entries);
  return (
    <div className="place-industry" data-testid={testId}>
      <table className="place-industry-bars">
        <caption>
          {heading}{period ? `, ${period}` : ""}{unit ? ` (${unit})` : ""}
          <span className="place-basis" data-testid={`${testId}-basis`}> · {rows[0]!.measure.basis}</span>
        </caption>
        <tbody>
          {bars.map((bar) => (
            <tr key={bar.id} data-testid={`${testId}-${bar.id}`}>
              <th scope="row">{bar.label}</th>
              <td>
                <span className="place-industry-track" aria-hidden="true">
                  <span className="place-industry-bar" style={{ width: `${max > 0 ? Math.max(0, (bar.value / max) * 100) : 0}%` }} />
                </span>
                <span className="place-industry-value">{formatObservationValue(bar.value)}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {gaps.map((gap) => (
        <p key={gap.reason} className="subtle place-industry-gaps">
          {gap.reason}:{" "}
          {gap.entries.map((entry, index) => (
            <span key={entry.id} data-testid={`${testId}-${entry.id}`}>
              {entry.label}{index < gap.entries.length - 1 ? ", " : "."}
            </span>
          ))}
        </p>
      ))}
    </div>
  );
}
