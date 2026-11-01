// "What the record shows" (D-9, ADR-0011).
//
// A place page answers four fixed, measurable questions, each with a chart
// and plain sentences built from templates over published numbers. The
// templates are the reviewed wording ADR-0011 accepts:
//
// - Only the verbs "rose", "fell", "was unchanged", "was higher than", "was
//   lower than" and "was not distinguishable from". No adjective of degree,
//   no causal word, no evaluative word (`FORBIDDEN_WORDS`, pinned by tests).
// - A survey estimate's change is "rose" or "fell" only when the two
//   90 percent intervals do not overlap; overlapping intervals are "was not
//   distinguishable from". A value published without a usable margin gets no
//   verb at all, because the rule cannot be applied to it.
// - Crime states who reported in each period, and says when coverage differs
//   enough that the change may reflect reporting. A period with no rate is
//   "not reported" or the program's own reason, never zero.
// - Rents and incomes are two series, stated separately and compared by
//   their percentages; neither is divided by the other.
// - Population change lists the components the Population Estimates Program
//   publishes and attributes the change to nothing.
//
// Every answer reads values the page already read for its chapters (their
// histories and newest values). The one read of its own is a county's crime
// rate history, computed from agency reports (ADR-0012), on county pages.

import { formatNumber } from "./format";
import type { DerivedRatioObservation } from "./derivedViews";
import { coverageText, unavailableText } from "./derivedViews";
import type { ObservationRow } from "./explorerViewModel";
import { publishedNumber } from "./explorerViewModel";
import { collapseToNewestRelease } from "./observationAccess";
import type { LevelAnswer, LevelPlace, PlaceLevel, ResolvedChapter, ResolvedPlaceMeasure, TrendModel } from "./placeChapters";
import { buildTrend, trendNotes, trendPeriodLabel } from "./placeChapters";

export type RecordQuestionId = "crime" | "rent-income" | "population" | "unemployment";

/** The questions, in the order the section asks them. */
export const RECORD_QUESTIONS: readonly { id: RecordQuestionId; question: string }[] = [
  { id: "crime", question: "Has violent crime gone up here in ten years?" },
  { id: "rent-income", question: "Are rents rising faster than incomes?" },
  { id: "population", question: "Is the population growing or shrinking, and why: births, deaths or migration?" },
  { id: "unemployment", question: "How does unemployment here compare with the state?" },
];

/** The unemployment question on a state page, where the larger area is the nation. */
export const UNEMPLOYMENT_NATION_QUESTION = "How does unemployment here compare with the nation?";

/** The verbs ADR-0011 rule 2 allows, and no others. */
export const RECORD_VERBS = [
  "rose",
  "fell",
  "was unchanged",
  "was higher than",
  "was lower than",
  "was not distinguishable from",
] as const;
export type ChangeVerb = "rose" | "fell" | "was unchanged" | "was not distinguishable from";
export type CompareVerb = "was higher than" | "was lower than" | "was not distinguishable from";

/**
 * Words a generated sentence must never hold (ADR-0011 rule 2): degree,
 * cause and judgement. Matched as whole words, case-insensitively.
 */
export const FORBIDDEN_WORDS = [
  "sharply", "dramatically", "significantly", "substantially", "slightly", "steeply", "soared", "plummeted", "surged", "spiked", "skyrocketed", "collapsed",
  "because", "due", "driven", "caused", "led", "thanks", "result",
  "better", "worse", "safer", "dangerous", "good", "bad", "improved", "worsened", "alarming", "troubling",
] as const;

/** Coverage that differs by more than this many percentage points makes a crime change "may reflect reporting". */
export const COVERAGE_DIFFERENCE_POINTS = 10;
/** How far back a change looks, at most. */
export const RECORD_SPAN_YEARS = 10;

export interface RecordChart {
  id: string;
  label: string;
  unit: string;
  model: TrendModel;
  notes: string[];
}

export interface RecordSource {
  /** What the series is, in the reader's words. */
  label: string;
  /** The identity it was read as, kept for the provenance record. */
  metricCode: string;
}

export interface RecordLink {
  label: string;
  href: string;
}

export interface RecordAnswer {
  id: RecordQuestionId;
  question: string;
  /**
   * `answered`: the sentences are built from two published values.
   * `unanswered`: the record cannot answer it here, and the sentences say why.
   * `elsewhere`: another page answers it (a city's county), linked.
   * `reading`: the one read of its own has not settled.
   */
  status: "answered" | "unanswered" | "elsewhere" | "reading";
  sentences: string[];
  coverage: string[];
  caveats: string[];
  sources: RecordSource[];
  charts: RecordChart[];
  links: RecordLink[];
}

// --- Numbers and periods ------------------------------------------------------

/** How a measure's value is written in a sentence. */
export type ValueKind = "dollars" | "percent" | "people" | "per100k";

export function valueText(value: number, kind: ValueKind): string {
  switch (kind) {
    case "dollars":
      return `${value < 0 ? "-" : ""}$${formatNumber(Math.abs(value), { maximumFractionDigits: 0 })}`;
    case "percent":
      return `${formatNumber(value, { maximumFractionDigits: 1 })} percent`;
    case "people":
      return formatNumber(value, { maximumFractionDigits: 0 });
    case "per100k":
      return `${formatNumber(value, { maximumFractionDigits: 1 })} per 100,000`;
  }
}

/** A change in the measure's own unit, signed. */
function changeText(change: number, kind: ValueKind): string {
  const sign = change > 0 ? "+" : "";
  if (kind === "per100k") return `${sign}${formatNumber(change, { maximumFractionDigits: 1 })} per 100,000`;
  return `${sign}${valueText(change, kind)}`;
}

/** Percent change from `from` to `to`, or null where it means nothing (a zero or negative base). */
export function percentChange(from: number, to: number): number | null {
  if (!(from > 0)) return null;
  return ((to - from) / from) * 100;
}

function percentText(percent: number): string {
  const sign = percent > 0 ? "+" : "";
  return `${sign}${formatNumber(percent, { maximumFractionDigits: 1, minimumFractionDigits: 1 })} percent`;
}

const MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];

/**
 * A period in the reader's words: a year, a five-year span ("2020–2024"), or
 * a month ("December 2024"). Anything else keeps the published start and end.
 */
export function recordPeriodLabel(row: Dated): string {
  const label = trendPeriodLabel(row as ObservationRow);
  if (/^\d{4}(–\d{4})?$/.test(label)) return label;
  const start = startOf(row);
  const end = String((row as { period_end?: unknown }).period_end || "");
  const month = /^(\d{4})-(\d{2})/.exec(start);
  if (month && end.slice(0, 7) === start.slice(0, 7)) {
    return `${MONTHS[Number(month[2]) - 1] || month[2]} ${month[1]}`;
  }
  return label;
}

/** Any row with a published period start: an observation or a derived row. */
type Dated = ObservationRow | DerivedRatioObservation;

const startOf = (row: Dated): string => String((row as { period_start?: unknown }).period_start || "");

function rowYear(row: Dated): number {
  return Number(startOf(row).slice(0, 4));
}

function rowMonth(row: Dated): string {
  return startOf(row).slice(5, 7);
}

function rowTime(row: Dated): number {
  return Date.parse(startOf(row));
}

/**
 * A survey value's published 90 percent margin, as a number, or null when
 * none is usable. The Bureau's "controlled" code is a margin of zero; its
 * other codes ("not computed", "open-ended median") are not numbers.
 */
export function surveyMargin(row: ObservationRow): number | null {
  const raw = row.margin_of_error;
  if (raw === undefined || raw === null || raw === "") return null;
  const number = Number(raw);
  if (number === -555555555) return 0;
  return Number.isFinite(number) && number >= 0 ? number : null;
}

// --- Verbs --------------------------------------------------------------------

/**
 * The verb for a change from `from` to `to` (ADR-0011 rules 2 and 3). With
 * margins, overlapping intervals are "was not distinguishable from"; without,
 * the published values decide. Returns null when only one margin is known,
 * because the rule cannot be applied.
 */
export function changeVerb(
  from: number,
  to: number,
  margins: { from: number | null; to: number | null } | null = null,
): ChangeVerb | null {
  if (margins) {
    if (margins.from === null || margins.to === null) return null;
    if (intervalsOverlap(from, margins.from, to, margins.to)) return "was not distinguishable from";
  }
  if (to > from) return "rose";
  if (to < from) return "fell";
  return "was unchanged";
}

/** The verb for one place beside another, by the same rules. */
export function compareVerb(
  value: number,
  other: number,
  margins: { value: number | null; other: number | null } | null = null,
): CompareVerb | null {
  if (margins) {
    if (margins.value === null || margins.other === null) return null;
    if (intervalsOverlap(value, margins.value, other, margins.other)) return "was not distinguishable from";
  }
  if (value > other) return "was higher than";
  if (value < other) return "was lower than";
  return "was not distinguishable from";
}

export function intervalsOverlap(a: number, aMargin: number, b: number, bMargin: number): boolean {
  return a - aMargin <= b + bMargin && b - bMargin <= a + aMargin;
}

/**
 * The 90 percent margin of a percent change from X1 to X2, by the Census
 * Bureau's formula for a ratio R = X2 / X1 (MOE(R) = sqrt(MOE2² + R²·MOE1²) / X1),
 * scaled to percent. Null when either margin is unknown or X1 is not positive.
 */
export function percentChangeMargin(from: number, fromMargin: number | null, to: number, toMargin: number | null): number | null {
  if (fromMargin === null || toMargin === null || !(from > 0)) return null;
  const ratio = to / from;
  return (Math.sqrt(toMargin ** 2 + ratio ** 2 * fromMargin ** 2) / from) * 100;
}

// --- Choosing the two periods -------------------------------------------------

export interface Span<Row> {
  from: Row;
  to: Row;
}

/**
 * The newest period with a number, and the earliest period with a number no
 * more than ten years before it and at least `minimumGap` years before it.
 * An ACS five-year series asks a gap of five, so the two periods share no
 * year (the Bureau's advice). Null when the record is too short.
 */
export function chooseSpan<Row extends ObservationRow>(rows: readonly Row[], minimumGap = 1): Span<Row> | null {
  const numbered = rows
    .filter((row) => publishedNumber(row.value) !== null && Number.isFinite(rowYear(row)))
    .sort((left, right) => rowTime(left) - rowTime(right));
  const to = numbered.at(-1);
  if (!to) return null;
  const toYear = rowYear(to);
  const from = numbered.find((row) => rowYear(row) >= toYear - RECORD_SPAN_YEARS && rowYear(row) <= toYear - minimumGap);
  return from ? { from, to } : null;
}

// --- Crime ----------------------------------------------------------------------

/** A county's crime rate history, as the section reads it. */
export type CountyCrimeHistory =
  | { state: "reading" }
  | { state: "failed"; message: string }
  | { state: "read"; rows: DerivedRatioObservation[]; apiPath: string };

function hasRate(row: DerivedRatioObservation): boolean {
  return !row.unavailable_reason && publishedNumber(row.value) !== null;
}

function agencyCount(row: DerivedRatioObservation): number {
  return row.coverage?.contributing_agencies?.length ?? 0;
}

function countyCoverageSentence(row: DerivedRatioObservation): string {
  const count = agencyCount(row);
  const agencies = `${formatNumber(count, { maximumFractionDigits: 0 })} ${count === 1 ? "agency" : "agencies"} reported`;
  const covers = coverageText(row.coverage);
  return `In ${recordPeriodLabel(row)}, ${agencies}${covers ? `; the rate ${covers}` : ""}.`;
}

function coveragePoints(row: DerivedRatioObservation): number | null {
  const share = Number(row.coverage?.coverage_share);
  return row.coverage?.coverage_share === null || row.coverage?.coverage_share === undefined || !Number.isFinite(share) ? null : share * 100;
}

/**
 * The county question: the newest month with a rate against the same month
 * up to ten years before it (ADR-0012 rates are monthly and never rolled up
 * into a year). Each month states who reported.
 */
export function countyCrimeAnswer(placeName: string, history: CountyCrimeHistory, place: LevelPlace): RecordAnswer {
  const base = blank("crime");
  base.sources = [{ label: "Violent crime rate computed from FBI agency reports (Return A), per 100,000 people the reporting agencies serve", metricCode: "FBI_UCR:return_a_violent_crime:V:offense:absolute_total" }];
  base.caveats = [
    "The rate is monthly, as agencies report it, and is not added up into a year. A month is compared with the same month of an earlier year.",
    "It divides the offenses reported by agencies serving only this county by the population those agencies serve; agencies that serve more than one county are left out (ADR-0012). Below 50 percent of that population, no rate is given.",
    "A month with no rate is not a month with no crime.",
  ];
  if (history.state === "reading") return { ...base, status: "reading", sentences: ["Reading this county's rate from agency reports…"] };
  if (history.state === "failed") return { ...base, status: "unanswered", sentences: [`This county's rate could not be read: ${history.message}`] };
  base.links = [{ label: "The rows this answer was made from (API)", href: history.apiPath }];
  const rows = [...history.rows].sort((left, right) => rowTime(left) - rowTime(right));
  const to = [...rows].reverse().find(hasRate);
  if (!to) {
    const newest = rows.at(-1);
    return {
      ...base,
      status: "unanswered",
      sentences: [newest
        ? `No month in the record has a rate for ${placeName}. In ${recordPeriodLabel(newest)}: ${unavailableText(newest.unavailable_reason).toLowerCase()}.`
        : `No agency report for ${placeName} is in the record, so no rate is given.`],
      coverage: newest ? [countyCoverageSentence(newest)] : [],
    };
  }
  const toYear = rowYear(to);
  const sameMonth = rows.filter((row) => rowMonth(row) === rowMonth(to) && rowYear(row) >= toYear - RECORD_SPAN_YEARS && rowYear(row) < toYear);
  const from = sameMonth.find(hasRate);
  const skipped = sameMonth.filter((row) => !hasRate(row) && (!from || rowYear(row) < rowYear(from)));
  const chart = crimeChart(place, rows);
  if (!from) {
    return {
      ...base,
      status: "unanswered",
      sentences: [
        `In ${recordPeriodLabel(to)}, the violent crime rate computed from agency reports for ${placeName} was ${valueText(Number(to.value), "per100k")} people its reporting agencies serve.`,
        sameMonth.length
          ? `No earlier ${MONTHS[Number(rowMonth(to)) - 1]} within ten years has a rate to compare it with.`
          : `The record holds no earlier ${MONTHS[Number(rowMonth(to)) - 1]} within ten years to compare it with.`,
      ],
      coverage: [countyCoverageSentence(to)],
      caveats: [...base.caveats, ...skippedCaveat(skipped)],
      charts: chart ? [chart] : [],
    };
  }
  const fromValue = Number(from.value);
  const toValue = Number(to.value);
  const verb = changeVerb(fromValue, toValue)!;
  const coverage = [countyCoverageSentence(from), countyCoverageSentence(to)];
  const fromPoints = coveragePoints(from);
  const toPoints = coveragePoints(to);
  if (fromPoints !== null && toPoints !== null && Math.abs(fromPoints - toPoints) > COVERAGE_DIFFERENCE_POINTS) {
    coverage.push(
      `Coverage was ${formatNumber(Math.floor(fromPoints), { maximumFractionDigits: 0 })} percent in ${recordPeriodLabel(from)} and ${formatNumber(Math.floor(toPoints), { maximumFractionDigits: 0 })} percent in ${recordPeriodLabel(to)}, a difference of more than ${COVERAGE_DIFFERENCE_POINTS} points, so the change may reflect which agencies reported.`,
    );
  }
  return {
    ...base,
    status: "answered",
    sentences: [changeSentence({
      measure: "The violent crime rate computed from agency reports",
      place: placeName,
      verb,
      from: { value: fromValue, period: recordPeriodLabel(from) },
      to: { value: toValue, period: recordPeriodLabel(to) },
      kind: "per100k",
      unitTail: " people its reporting agencies serve",
    })],
    coverage,
    caveats: [...base.caveats, ...skippedCaveat(skipped)],
    charts: chart ? [chart] : [],
  };
}

function skippedCaveat(skipped: DerivedRatioObservation[]): string[] {
  if (!skipped.length) return [];
  return [`Earlier months with no rate, so not compared: ${skipped.map((row) => `${recordPeriodLabel(row)} (${unavailableText(row.unavailable_reason).toLowerCase()})`).join("; ")}.`];
}

function crimeChart(place: LevelPlace, rows: DerivedRatioObservation[]): RecordChart | null {
  if (!rows.length) return null;
  const asRows: ObservationRow[] = rows.map((row) => ({
    geo_id: row.geo_id,
    metric_code: row.ratio_id,
    value: hasRate(row) ? row.value ?? null : null,
    period_start: row.period_start,
    period_end: row.period_end,
  }));
  return {
    id: "crime",
    label: "Violent crime rate computed from agency reports",
    unit: "offenses per 100,000 people the reporting agencies serve, each month",
    model: buildTrend([place], new Map([[place.geoId, asRows]]), "level"),
    notes: ["A month with no point is one with no rate: too few agencies reported, or none did."],
  };
}

/**
 * A state's or the nation's question, from the FBI's own published rate.
 * The FBI publishes the share of the population its participating agencies
 * cover; that is stated for each year.
 */
export function publishedCrimeAnswer(
  place: LevelPlace,
  measure: ResolvedPlaceMeasure,
  rows: readonly ObservationRow[],
  link: string,
): RecordAnswer {
  const base = blank("crime");
  base.sources = [{ label: "FBI Uniform Crime Reporting Program, violent crime rate as the program published it", metricCode: measure.metricCode }];
  base.links = link ? [{ label: "The rows this answer was made from", href: link }] : [];
  base.caveats = ["The program's own rate on its own population; nothing here divides a count by a population. A year no agency reported is not a year with no crime."];
  const settled = collapseToNewestRelease([...rows]);
  const span = chooseSpan(settled);
  const chart: RecordChart = {
    id: "crime",
    label: "Violent crime rate, as the program published it",
    unit: "offenses per 100,000 residents",
    model: buildTrend([place], new Map([[place.geoId, settled]]), "level"),
    notes: [],
  };
  if (!span) {
    return { ...base, status: "unanswered", sentences: [tooShort(place.name, "the FBI's violent crime rate")], charts: settled.length ? [chart] : [] };
  }
  const fromValue = Number(span.from.value);
  const toValue = Number(span.to.value);
  const coverage = [span.from, span.to].map((row) => {
    const percent = Number((row.coverage as Record<string, unknown> | null)?.coverage_percent);
    return Number.isFinite(percent) && (row.coverage as Record<string, unknown> | null)?.coverage_percent !== null
      ? `In ${recordPeriodLabel(row)}, the agencies reporting to the FBI covered ${formatNumber(percent, { maximumFractionDigits: 1 })} percent of the population, as the FBI published it.`
      : `In ${recordPeriodLabel(row)}, the FBI published no coverage for this rate.`;
  });
  const percents = [span.from, span.to].map((row) => Number((row.coverage as Record<string, unknown> | null)?.coverage_percent));
  if (percents.every(Number.isFinite) && Math.abs(percents[0]! - percents[1]!) > COVERAGE_DIFFERENCE_POINTS) {
    coverage.push(`Coverage differed by more than ${COVERAGE_DIFFERENCE_POINTS} percentage points between the two years, so the change may reflect which agencies reported.`);
  }
  return {
    ...base,
    status: "answered",
    sentences: [changeSentence({
      measure: "The FBI's violent crime rate",
      place: place.name,
      verb: changeVerb(fromValue, toValue)!,
      from: { value: fromValue, period: recordPeriodLabel(span.from) },
      to: { value: toValue, period: recordPeriodLabel(span.to) },
      kind: "per100k",
      unitTail: " residents",
    })],
    coverage,
    charts: [chart],
  };
}

// --- Sentences ------------------------------------------------------------------

interface ChangeParts {
  measure: string;
  place: string;
  verb: ChangeVerb;
  from: { value: number; period: string };
  to: { value: number; period: string };
  kind: ValueKind;
  /** Words after a rate's unit ("people its reporting agencies serve"). */
  unitTail?: string;
}

/**
 * The change template: the measure, the place, the two periods, the two
 * values, the change in the measure's unit and in percent. An unchanged or
 * indistinguishable change states both values and no change figure.
 */
export function changeSentence(parts: ChangeParts): string {
  const tail = parts.unitTail || "";
  const from = `${valueText(parts.from.value, parts.kind)}${tail}`;
  const to = `${valueText(parts.to.value, parts.kind)}${tail}`;
  if (parts.verb === "was not distinguishable from") {
    return `${parts.measure} in ${parts.place} was ${from} in ${parts.from.period} and ${to} in ${parts.to.period}; the ${parts.to.period} value was not distinguishable from the ${parts.from.period} value at the published 90 percent margins of error.`;
  }
  if (parts.verb === "was unchanged") {
    return `${parts.measure} in ${parts.place} was unchanged at ${from} in ${parts.from.period} and in ${parts.to.period}.`;
  }
  const change = parts.to.value - parts.from.value;
  const percent = parts.kind === "percent" ? null : percentChange(parts.from.value, parts.to.value);
  const changeWords = `${changeText(change, parts.kind)}${percent === null ? "" : ` (${percentText(percent)})`}`;
  return `${parts.measure} in ${parts.place} ${parts.verb} from ${from} in ${parts.from.period} to ${to} in ${parts.to.period}, a change of ${changeWords}.`;
}

function tooShort(place: string, measure: string): string {
  return `The record for ${place} is too short to compare ${measure} across years: it holds no two published periods within ten years of each other.`;
}

function blank(id: RecordQuestionId): RecordAnswer {
  const question = RECORD_QUESTIONS.find((item) => item.id === id)!.question;
  return { id, question, status: "unanswered", sentences: [], coverage: [], caveats: [], sources: [], charts: [], links: [] };
}

// --- Rents and incomes ------------------------------------------------------------

function surveyLabel(metricCode: string): string {
  if (metricCode.startsWith("CENSUS_ACS:acs5:")) return "American Community Survey 5-year estimates";
  if (metricCode.startsWith("CENSUS_ACS:acs1:")) return "American Community Survey 1-year estimates";
  return "American Community Survey";
}

function datasetOf(metricCode: string): string {
  return metricCode.split(":")[1] || "";
}

/**
 * Rents and incomes (ADR-0011 rule 5): each series' change from the same
 * program, dataset and periods, stated separately, and the two percentages
 * compared. Each change's verb follows its margins; the comparison of the
 * percentages follows the margins of each percent change.
 */
export function rentIncomeAnswer(
  place: LevelPlace,
  rent: ResolvedPlaceMeasure | null,
  income: ResolvedPlaceMeasure | null,
  histories: ReadonlyMap<string, ObservationRow[]>,
  links: RecordLink[],
): RecordAnswer {
  const base = blank("rent-income");
  if (!rent?.metric || !income?.metric) {
    return { ...base, status: "unanswered", sentences: ["This warehouse does not publish both median gross rent and median household income."] };
  }
  base.sources = [
    { label: `Median gross rent, ${surveyLabel(rent.metricCode)}`, metricCode: rent.metricCode },
    { label: `Median household income, ${surveyLabel(income.metricCode)}`, metricCode: income.metricCode },
  ];
  base.links = links;
  base.caveats = [
    "Rent is the median gross rent (rent plus the utilities the renter pays) of renter households; income is the median income of all households. They are two different medians, compared only by how much each changed.",
    "Both are in each period's own dollars, so each change includes inflation. Converting both to one year's dollars would change both percentages by the same factor, not which is larger.",
    "Neither series is divided by the other; no affordability index is computed.",
  ];
  const rentRows = histories.get(key(rent.metricCode, place.geoId)) || [];
  const incomeRows = histories.get(key(income.metricCode, place.geoId)) || [];
  const charts: RecordChart[] = [
    surveyChart("rent", "Median gross rent", rent.metricCode, place, rentRows),
    surveyChart("income", "Median household income", income.metricCode, place, incomeRows),
  ].filter((chart): chart is RecordChart => chart !== null);
  // The two charts share their notes; the last one states them once.
  charts.slice(0, -1).forEach((chart) => { chart.notes = []; });
  if (datasetOf(rent.metricCode) !== datasetOf(income.metricCode)) {
    return { ...base, status: "unanswered", charts, sentences: ["Rent and income are published here from different survey datasets, so their changes are not compared."] };
  }
  const gap = rent.metricCode.startsWith("CENSUS_ACS:acs5:") ? 5 : 1;
  const span = chooseSpan(rentRows, gap);
  if (!span) return { ...base, status: "unanswered", charts, sentences: [tooShort(place.name, "median gross rent")] };
  const period = (row: ObservationRow) => recordPeriodLabel(row);
  const incomeFrom = incomeRows.find((row) => period(row) === period(span.from) && publishedNumber(row.value) !== null);
  const incomeTo = incomeRows.find((row) => period(row) === period(span.to) && publishedNumber(row.value) !== null);
  if (!incomeFrom || !incomeTo) {
    const missing = [!incomeFrom ? period(span.from) : "", !incomeTo ? period(span.to) : ""].filter(Boolean).join(" or ");
    return {
      ...base,
      status: "unanswered",
      charts,
      sentences: [`Median household income for ${place.name} was not published for ${missing}, the periods rent is compared over, so the two are not compared.`],
    };
  }
  const series = [
    { name: "Median gross rent", from: span.from, to: span.to },
    { name: "Median household income", from: incomeFrom, to: incomeTo },
  ].map((item) => {
    const from = Number(item.from.value);
    const to = Number(item.to.value);
    const fromMargin = surveyMargin(item.from);
    const toMargin = surveyMargin(item.to);
    return {
      ...item,
      fromValue: from,
      toValue: to,
      verb: changeVerb(from, to, { from: fromMargin, to: toMargin }),
      percent: percentChange(from, to),
      percentMargin: percentChangeMargin(from, fromMargin, to, toMargin),
    };
  });
  const sentences = series.map((item) =>
    item.verb
      ? changeSentence({
          measure: item.name,
          place: place.name,
          verb: item.verb,
          from: { value: item.fromValue, period: period(item.from) },
          to: { value: item.toValue, period: period(item.to) },
          kind: "dollars",
        })
      : `${item.name} in ${place.name} was ${valueText(item.fromValue, "dollars")} in ${period(item.from)} and ${valueText(item.toValue, "dollars")} in ${period(item.to)}; the Bureau published no usable margin of error for one of them, so no change is stated.`,
  );
  const [rentSeries, incomeSeries] = series as [typeof series[number], typeof series[number]];
  if (rentSeries.percent !== null && incomeSeries.percent !== null) {
    const verb = compareVerb(rentSeries.percent, incomeSeries.percent, { value: rentSeries.percentMargin, other: incomeSeries.percentMargin });
    sentences.push(
      verb
        ? `From ${period(span.from)} to ${period(span.to)}, the percent change in median gross rent (${percentText(rentSeries.percent)}) ${verb} the percent change in median household income (${percentText(incomeSeries.percent)})${verb === "was not distinguishable from" ? " at the published margins of error" : ""}.`
        : `From ${period(span.from)} to ${period(span.to)}, median gross rent changed ${percentText(rentSeries.percent)} and median household income ${percentText(incomeSeries.percent)}; without every margin of error the two changes are not compared.`,
    );
  }
  return { ...base, status: "answered", sentences, charts };
}

function surveyChart(id: string, label: string, metricCode: string, place: LevelPlace, rows: ObservationRow[]): RecordChart | null {
  if (!rows.length) return null;
  return {
    id,
    label: `${label} in ${place.name}`,
    unit: "dollars",
    model: buildTrend([place], new Map([[place.geoId, rows]]), "level"),
    notes: trendNotes(metricCode, "dollars"),
  };
}

// --- Population ---------------------------------------------------------------------

/** The component slots of the Change chapter, in the order a sentence names them. */
export const COMPONENT_MEASURES = ["births", "deaths", "domestic-migration", "international-migration"] as const;

/**
 * Population change (ADR-0011 rule 6): the Population Estimates Program's
 * estimate over up to ten years, then the components it publishes for its
 * newest year, as published. Nothing is attributed beyond them.
 */
export function populationAnswer(
  place: LevelPlace,
  estimate: ResolvedPlaceMeasure | null,
  components: ReadonlyMap<string, { measure: ResolvedPlaceMeasure; row: ObservationRow | null }>,
  histories: ReadonlyMap<string, ObservationRow[]>,
  options: { componentsElsewhere?: string | null; links: RecordLink[] },
): RecordAnswer {
  const base = blank("population");
  if (!estimate?.metric) {
    return { ...base, status: "unanswered", sentences: ["This warehouse does not publish the Population Estimates Program's resident population estimate."] };
  }
  base.sources = [{ label: "Census Bureau Population Estimates Program, resident population", metricCode: estimate.metricCode }];
  base.links = options.links;
  base.caveats = ["Population estimates are the program's, not a count; each year's figure is the newest vintage published for it."];
  const rows = collapseToNewestRelease([...(histories.get(key(estimate.metricCode, place.geoId)) || [])]);
  const chart: RecordChart | null = rows.length
    ? { id: "population", label: `Resident population estimate, ${place.name}`, unit: "people", model: buildTrend([place], new Map([[place.geoId, rows]]), "level"), notes: [] }
    : null;
  const span = chooseSpan(rows);
  const sentences: string[] = [];
  let status: RecordAnswer["status"] = "answered";
  if (!span) {
    sentences.push(tooShort(place.name, "the population estimate"));
    status = "unanswered";
  } else {
    sentences.push(changeSentence({
      measure: "The estimated resident population",
      place: place.name,
      verb: changeVerb(Number(span.from.value), Number(span.to.value))!,
      from: { value: Number(span.from.value), period: recordPeriodLabel(span.from) },
      to: { value: Number(span.to.value), period: recordPeriodLabel(span.to) },
      kind: "people",
    }));
    const vintages = [span.from.release, span.to.release].map((release) => String(release ?? ""));
    if (vintages[0] && vintages[1] && vintages[0] !== vintages[1]) {
      base.caveats.push(`The two years come from different vintages of the estimates (${vintages[0]} and ${vintages[1]}); a later vintage revises earlier years.`);
    }
  }
  if (options.componentsElsewhere) {
    sentences.push(`The program publishes births, deaths and migration for counties, states and the nation, not for cities and towns; ${options.componentsElsewhere}'s components are on its page.`);
  } else {
    const parts = COMPONENT_MEASURES.map((id) => components.get(id)).filter((entry): entry is { measure: ResolvedPlaceMeasure; row: ObservationRow | null } => Boolean(entry));
    const numbered = parts.filter((entry) => entry.row && publishedNumber(entry.row.value) !== null);
    if (!numbered.length) {
      sentences.push(`The program's components of change (births, deaths and migration) are not published for ${place.name} here.`);
    } else {
      for (const part of numbered) base.sources.push({ label: `Population Estimates Program, ${part.measure.measure.label.toLowerCase()}`, metricCode: part.measure.metricCode });
      const byPeriod = new Map<string, string[]>();
      for (const part of numbered) {
        const label = recordPeriodLabel(part.row!);
        const list = byPeriod.get(label) || [];
        list.push(componentWords(part.measure.measure.id, Number(part.row!.value)));
        byPeriod.set(label, list);
      }
      for (const [period, words] of byPeriod) {
        sentences.push(`For ${period}, the program estimates ${listWords(words)} in ${place.name}.`);
      }
      const missing = parts.filter((entry) => !numbered.includes(entry)).map((entry) => entry.measure.measure.label.toLowerCase());
      if (missing.length) sentences.push(`Not published for ${place.name}: ${listWords(missing)}.`);
      base.caveats.push("Components are estimates for the year ending July 1 of the vintage. The program also publishes a residual, which is not a component and is not shown. These are the only parts of the change the program publishes; nothing else is attributed here.");
    }
  }
  return { ...base, status, sentences, charts: chart ? [chart] : [] };
}

function componentWords(id: string, value: number): string {
  const amount = valueText(value, "people");
  switch (id) {
    case "births": return `${amount} births`;
    case "deaths": return `${amount} deaths`;
    case "domestic-migration": return `net domestic migration of ${amount}`;
    case "international-migration": return `net international migration of ${amount}`;
    default: return amount;
  }
}

function listWords(words: string[]): string {
  if (words.length <= 1) return words.join("");
  return `${words.slice(0, -1).join(", ")} and ${words.at(-1)}`;
}

// --- Unemployment -----------------------------------------------------------------------

/**
 * This place beside its state (or a state beside the nation), in the newest
 * period both publish, on the same seasonal-adjustment basis.
 */
export function unemploymentAnswer(
  place: LevelPlace,
  other: LevelPlace | null,
  measure: ResolvedPlaceMeasure | null,
  histories: ReadonlyMap<string, ObservationRow[]>,
  links: RecordLink[],
  question: string,
): RecordAnswer {
  const base = { ...blank("unemployment"), question };
  if (!measure?.metric) {
    return { ...base, status: "unanswered", sentences: ["This warehouse does not publish the BLS unemployment rate."] };
  }
  if (!other) {
    return { ...base, status: "unanswered", sentences: ["The nation has no larger area to compare its unemployment rate with."] };
  }
  base.sources = [{ label: "BLS Local Area Unemployment Statistics, unemployment rate", metricCode: measure.metricCode }];
  base.links = links;
  base.caveats = ["Local Area Unemployment Statistics are BLS estimates published without a margin of error, so the comparison is of the published values."];
  const own = collapseToNewestRelease([...(histories.get(key(measure.metricCode, place.geoId)) || [])]);
  const theirs = collapseToNewestRelease([...(histories.get(key(measure.metricCode, other.geoId)) || [])]);
  const lines = [place, other].filter((entry) => (entry === place ? own : theirs).length);
  const chart: RecordChart | null = lines.length
    ? {
        id: "unemployment",
        label: `Unemployment rate, ${place.name} and ${other.name}`,
        unit: "percent",
        model: buildTrend(lines, new Map([[place.geoId, own], [other.geoId, theirs]]), "level"),
        notes: [],
      }
    : null;
  const charts = chart ? [chart] : [];
  const numbered = (rows: ObservationRow[]) => rows.filter((row) => publishedNumber(row.value) !== null);
  const otherByPeriod = new Map(numbered(theirs).map((row) => [recordPeriodLabel(row), row]));
  const shared = numbered(own)
    .filter((row) => otherByPeriod.has(recordPeriodLabel(row)))
    .sort((left, right) => rowTime(left) - rowTime(right))
    .at(-1);
  if (!shared) {
    return { ...base, status: "unanswered", charts, sentences: [`${place.name} and ${other.name} have no period in which BLS published both unemployment rates here.`] };
  }
  const match = otherByPeriod.get(recordPeriodLabel(shared))!;
  const basis = (row: ObservationRow) => String(((row.dimensions || {}) as Record<string, unknown>).seasonal_adjustment_status ?? "");
  if (basis(shared) && basis(match) && basis(shared) !== basis(match)) {
    return {
      ...base,
      status: "unanswered",
      charts,
      sentences: [`BLS published the two rates for ${recordPeriodLabel(shared)} on different seasonal-adjustment bases (${basis(shared)} and ${basis(match)}), so they are not compared.`],
    };
  }
  const value = Number(shared.value);
  const otherValue = Number(match.value);
  const verb = compareVerb(value, otherValue)!;
  return {
    ...base,
    status: "answered",
    charts,
    sentences: [
      `In ${recordPeriodLabel(shared)}, the unemployment rate in ${place.name} was ${valueText(value, "percent")} and in ${other.name} ${valueText(otherValue, "percent")}.`,
      `${place.name}'s rate ${verb} ${other.name}'s.`,
    ],
  };
}

// --- The section ----------------------------------------------------------------------------

const key = (metricCode: string, geoId: string) => `${metricCode}|${geoId}`;

/** The chapters' resolved measures, by slot id. */
export function measuresById(shown: readonly ResolvedChapter[]): Map<string, ResolvedPlaceMeasure> {
  const found = new Map<string, ResolvedPlaceMeasure>();
  for (const resolved of shown) {
    for (const measure of [...resolved.headline, ...resolved.depth]) {
      if (measure.metric && !found.has(measure.measure.id)) found.set(measure.measure.id, measure);
    }
  }
  return found;
}

export interface RecordInputs {
  level: PlaceLevel;
  /** This place first, then the levels above it, as the cards read them. */
  levels: readonly LevelPlace[];
  shown: readonly ResolvedChapter[];
  histories: ReadonlyMap<string, ObservationRow[]>;
  answers: ReadonlyMap<string, LevelAnswer>;
  /** A county page's crime rate history; null on every other page. */
  countyCrime: CountyCrimeHistory | null;
  /** A city's county, where a question is answered for the county and not the city. */
  countyPage: { name: string; href: string } | null;
  /** The explorer link for a measure at a place. */
  exploreLink: (measure: ResolvedPlaceMeasure, place: LevelPlace) => string;
}

/** Every question's answer for one place page. */
export function recordAnswers(inputs: RecordInputs): RecordAnswer[] {
  const { level, levels, shown, histories, answers, countyCrime, countyPage, exploreLink } = inputs;
  const own = levels[0];
  if (!own) return [];
  const measures = measuresById(shown);
  const link = (id: string, place: LevelPlace = own): RecordLink[] => {
    const measure = measures.get(id);
    const href = measure ? exploreLink(measure, place) : "";
    return href ? [{ label: `${measure!.measure.label} in the explorer`, href }] : [];
  };
  const elsewhere = (id: RecordQuestionId, sentence: string): RecordAnswer => ({
    ...blank(id),
    status: countyPage ? "elsewhere" : "unanswered",
    sentences: [sentence],
    links: countyPage ? [{ label: `${countyPage.name}'s answer`, href: `${countyPage.href}#record` }] : [],
  });

  // Crime.
  let crime: RecordAnswer;
  if (level === "COUNTY") {
    crime = countyCrime ? countyCrimeAnswer(own.name, countyCrime, own) : { ...blank("crime"), sentences: ["No rate computed from agency reports is served for counties here."] };
  } else if (level === "STATE" || level === "NATIONAL") {
    const measure = measures.get("violent-crime-rate") || null;
    crime = measure
      ? publishedCrimeAnswer(own, measure, histories.get(key(measure.metricCode, own.geoId)) || [], link("violent-crime-rate")[0]?.href || "")
      : { ...blank("crime"), sentences: ["This warehouse does not publish the FBI's violent crime rate."] };
  } else if (level === "PLACE") {
    crime = elsewhere("crime", "The FBI publishes reports by law-enforcement agency, state and the nation, not by city or town, and this site computes a rate from agency reports for counties only.");
  } else {
    crime = { ...blank("crime"), sentences: ["No violent crime rate is published or computed for metro areas."] };
  }

  // Rents and incomes.
  const rentIncome = rentIncomeAnswer(own, measures.get("median-gross-rent") || null, measures.get("median-household-income") || null, histories, [
    ...link("median-gross-rent"),
    ...link("median-household-income"),
  ]);

  // Population.
  const components = new Map<string, { measure: ResolvedPlaceMeasure; row: ObservationRow | null }>();
  for (const id of COMPONENT_MEASURES) {
    const measure = measures.get(id);
    if (measure) components.set(id, { measure, row: answers.get(key(measure.metricCode, own.geoId))?.row ?? null });
  }
  const population = populationAnswer(own, measures.get("population-estimate") || null, components, histories, {
    componentsElsewhere: level === "PLACE" ? countyPage?.name || "its county" : null,
    links: [...link("population-estimate"), ...(level === "PLACE" && countyPage ? [{ label: `${countyPage.name}'s components`, href: `${countyPage.href}#change` }] : [])],
  });

  // Unemployment.
  let unemployment: RecordAnswer;
  if (level === "PLACE") {
    unemployment = elsewhere("unemployment", "BLS publishes the unemployment rate for counties, metro areas, states and the nation, not for cities and towns.");
  } else {
    const other = level === "STATE"
      ? levels.find((entry) => entry.level === "NATIONAL") || null
      : level === "NATIONAL" ? null : levels.find((entry) => entry.level === "STATE") || null;
    unemployment = unemploymentAnswer(own, other, measures.get("unemployment-rate") || null, histories, link("unemployment-rate"),
      level === "STATE" ? UNEMPLOYMENT_NATION_QUESTION : RECORD_QUESTIONS[3]!.question);
  }
  return [crime, rentIncome, population, unemployment];
}

/** Every word of an answer a reader sees as generated text, for the wording checks. */
export function generatedText(answer: RecordAnswer): string {
  return [...answer.sentences, ...answer.coverage].join(" ");
}

/** Forbidden words a text holds (ADR-0011 rule 2); empty when it holds none. */
export function forbiddenWordsIn(text: string): string[] {
  const words = new Set(String(text).toLowerCase().match(/[a-z]+/g) || []);
  return FORBIDDEN_WORDS.filter((word) => words.has(word));
}
