// Age and sex structure (web-age-and-sex-structure, ADR-0013).
//
// The fetchers for `/age-structure`, `/age-structure/history` and
// `/age-structure/cohorts`, the comparison toggle model, and the formatting
// the pyramid, the cards, the difference bars, the history and the cohorts
// share. Every count, share, margin and ratio is the API's: this module only
// reads them, subtracts one served share from another for the "where it
// differs" chart, and words them. It never sums lines into bands, never
// recomputes a margin and never estimates a median from the bars.
//
// The toggle (Nick, 2026-10-31): the references the API returned, in the
// order it returned them (next level up first), plus "None". One choice
// drives the pyramid's outline, the cards' comparison values and the
// difference bars, all from the one answer already held, so switching makes
// no request.

import { apiFetch } from "./api/client";
import type { RequestOptions } from "./api/client";
import { formatNumber } from "./format";

/** A served decimal: a string in the neutral contract's form, or null when unavailable. */
export type Served = string | null;

export interface AgeScheme {
  scheme_id: string;
  version: number;
  status: string;
}

export interface AgeCount {
  count: Served;
  count_moe: Served;
}

export interface AgeBandSide extends AgeCount {
  share: Served;
  share_moe: Served;
  cv_percent: Served;
  reliable: boolean;
  unavailable_reason: string | null;
}

export interface AgeBand {
  band_id: string;
  age_start: number;
  age_end: number | null;
  label: string;
  lines: { men: string[]; women: string[] };
  men: AgeBandSide;
  women: AgeBandSide;
}

export interface AgeValue {
  value: Served;
  moe: Served;
  moe_method?: string;
  unit?: string;
  unavailable_reason?: string | null;
}

export interface AgeGroupIndicator extends AgeCount {
  share: Served;
  share_moe: Served;
}

export interface AgeIndicators {
  under_18: AgeGroupIndicator;
  age_18_64: AgeGroupIndicator;
  age_65_plus: AgeGroupIndicator;
  age_85_plus: AgeGroupIndicator;
  child_dependency_ratio: AgeValue;
  old_age_dependency_ratio: AgeValue;
  total_dependency_ratio: AgeValue;
  sex_ratio: AgeValue;
}

export type AgeRole = "place" | "reference" | "compare";
export type ReferenceLevel = "COUNTY" | "STATE" | "NATIONAL";

/** One place's structure: the requested place, a reference, or a compared place. */
export interface AgePlace {
  geo_id: string;
  geo_level: string;
  geo_name: string;
  role: AgeRole;
  reference_level: ReferenceLevel | null;
  /** A number on the wire; read through `num`. */
  overlap_weight: Served | number;
  release: string | null;
  period_start: string | null;
  period_end: string | null;
  unavailable_reason: string | null;
  total: AgeCount | null;
  men: AgeCount | null;
  women: AgeCount | null;
  bands: AgeBand[];
  median_age: { all: AgeValue; men: AgeValue; women: AgeValue } | null;
  indicators: AgeIndicators | null;
  derived: boolean;
}

export interface AgeStructureResponse {
  geo_id: string;
  geo_level: string;
  geo_name: string;
  dataset: string;
  scheme: AgeScheme;
  default_reference_geo_id: string | null;
  places: AgePlace[];
}

export interface AgeHistoryResponse {
  geo_id: string;
  geo_level: string;
  geo_name: string;
  dataset: string;
  scheme: AgeScheme;
  periods: "non_overlapping" | "all";
  items: AgePlace[];
}

export interface CohortSide {
  from_count: Served;
  to_count: Served;
  change: Served;
  change_moe: Served;
  /** Null when a margin is missing, so the intervals cannot be compared. */
  intervals_overlap: boolean | null;
  from_count_moe?: Served;
  to_count_moe?: Served;
}

export interface CohortItem {
  from_band: string;
  to_band: string;
  men: CohortSide;
  women: CohortSide;
}

export interface CohortPeriod {
  release: string;
  period_start: string;
  period_end: string;
}

export interface CohortRefusal {
  reason: "overlapping_periods" | "period_gap_not_five_years" | "boundary_changed" | "not_published" | string;
  detail?: string | null;
}

export interface AgeCohortsResponse {
  geo_id: string;
  dataset: string;
  scheme: AgeScheme;
  /** Null when the API refused before it could name the periods. */
  from: CohortPeriod | null;
  to: CohortPeriod | null;
  years_apart: number | null;
  comparable: boolean;
  refusal: CohortRefusal | null;
  caveat: string;
  items: CohortItem[];
}

export type AgeDataset = "acs5" | "acs1";
export type Sex = "men" | "women";
export const SEXES: readonly Sex[] = ["men", "women"];

// ---------------------------------------------------------------- fetchers

export function getAgeStructure(
  params: { geo_id: string; dataset?: AgeDataset; release?: string; compare_geo_ids?: readonly string[] },
  options: RequestOptions = {},
): Promise<AgeStructureResponse> {
  return apiFetch<AgeStructureResponse>("/age-structure", {
    ...options,
    params: {
      geo_id: params.geo_id,
      dataset: params.dataset,
      release: params.release,
      compare_geo_ids: params.compare_geo_ids?.length ? params.compare_geo_ids.join(",") : undefined,
    },
  });
}

export function getAgeStructureHistory(
  params: { geo_id: string; dataset?: AgeDataset; periods?: "non_overlapping" | "all"; include_bands?: boolean },
  options: RequestOptions = {},
): Promise<AgeHistoryResponse> {
  return apiFetch<AgeHistoryResponse>("/age-structure/history", {
    ...options,
    params: {
      geo_id: params.geo_id,
      dataset: params.dataset,
      periods: params.periods,
      include_bands: params.include_bands === undefined ? undefined : String(params.include_bands),
    },
  });
}

export function getAgeCohorts(
  params: { geo_id: string; dataset?: AgeDataset; from_release: string; to_release: string },
  options: RequestOptions = {},
): Promise<AgeCohortsResponse> {
  return apiFetch<AgeCohortsResponse>("/age-structure/cohorts", { ...options, params });
}

// ---------------------------------------------------------------- numbers

/** A served decimal as a number; null stays null, never zero. */
export function num(value: Served | number | undefined): number | null {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function percentText(value: Served | number | null | undefined, digits = 1): string {
  const parsed = typeof value === "number" ? value : num(value ?? null);
  return parsed === null ? "not available" : `${formatNumber(parsed, { minimumFractionDigits: digits, maximumFractionDigits: digits })}%`;
}

export function countText(value: Served | undefined): string {
  const parsed = num(value ?? null);
  return parsed === null ? "not available" : formatNumber(parsed, { maximumFractionDigits: 0 });
}

export function decimalText(value: Served | undefined, digits = 1): string {
  const parsed = num(value ?? null);
  return parsed === null ? "not available" : formatNumber(parsed, { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

/** "± 0.2" for a served margin; empty when the API served none. */
export function marginText(moe: Served | undefined, digits = 1): string {
  const parsed = num(moe ?? null);
  return parsed === null ? "" : `± ${formatNumber(parsed, { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
}

/** Signed percentage points with a true minus sign. */
export function signedPoints(value: number | null, digits = 2): string {
  if (value === null) return "not available";
  const rounded = Number(value.toFixed(digits));
  const sign = rounded > 0 ? "+" : rounded < 0 ? "−" : "";
  return `${sign}${formatNumber(Math.abs(rounded), { minimumFractionDigits: digits, maximumFractionDigits: digits })}`;
}

/** "2019–2023" from a served period, or the single year of a one-year release. */
export function periodText(place: Pick<AgePlace, "period_start" | "period_end" | "release">): string {
  const start = place.period_start?.slice(0, 4);
  const end = place.period_end?.slice(0, 4);
  if (start && end) return start === end ? end : `${start}–${end}`;
  return place.release || "no period";
}

/** A band's label as a reader says it: "0 to 4", "85 and over". */
export function bandText(band: Pick<AgeBand, "label" | "age_start" | "age_end">): string {
  if (band.label) return band.label;
  return band.age_end === null ? `${band.age_start} and over` : `${band.age_start} to ${band.age_end}`;
}

/** A band id ("25-29", "85+") as words, for the cohort rows that carry only ids. */
export function bandIdText(bandId: string): string {
  if (bandId.endsWith("+")) return `${bandId.slice(0, -1)} and over`;
  const [start, end] = bandId.split("-");
  return end ? `${start} to ${end}` : bandId;
}

export function sexWord(sex: Sex, capital = false): string {
  const word = sex === "men" ? "men" : "women";
  return capital ? word[0]!.toUpperCase() + word.slice(1) : word;
}

const UNAVAILABLE_WORDS: Record<string, string> = {
  suppressed_input: "the Census Bureau withheld one of the published lines this group is made of",
  line_missing: "one of the published lines this group is made of is not loaded",
  total_missing: "the place's total population is not published, so no share can be worked out",
  denominator_zero: "nobody aged 18 to 64 is counted here, so the ratio cannot be worked out",
  not_published: "not published for this survey period",
  release_mismatch: "published for a different survey period, so not drawn beside this one",
};

/** Why a band, a group or a place is unavailable, in words. */
export function unavailableText(reason: string | null | undefined): string {
  if (!reason) return "not available";
  return UNAVAILABLE_WORDS[reason] || "not available";
}

export function isDraftScheme(scheme: Pick<AgeScheme, "status"> | null | undefined): boolean {
  return (scheme?.status || "draft") !== "approved";
}

export const DRAFT_SCHEME_TEXT = "draft";
export function draftSchemeNote(scheme: AgeScheme): string {
  return `Computed here from the published age lines under the five-year grouping (version ${scheme.version}), which has not been reviewed yet.`;
}

// ---------------------------------------------------------------- the toggle

export const NO_REFERENCE = "none";

export interface ReferenceChoice {
  /** The reference's geo_id, or `none`. */
  key: string;
  /** "County", "State", "Nation", "None". */
  levelWord: string;
  /** The reference's name; empty for None. */
  name: string;
  place: AgePlace | null;
  /** "87 % of Madison's area lies in Dane County", when a city's county overlap is below 100 %. */
  overlapNote: string | null;
}

const LEVEL_WORDS: Record<string, string> = { COUNTY: "County", STATE: "State", NATIONAL: "Nation" };

export function placeOf(answer: AgeStructureResponse): AgePlace | null {
  return answer.places.find((item) => item.role === "place") || answer.places.find((item) => item.geo_id === answer.geo_id) || null;
}

export function comparedPlaces(answer: AgeStructureResponse): AgePlace[] {
  return answer.places.filter((item) => item.role === "compare");
}

/** What the share of a city's area inside its county reads as, below 100 % only. */
export function overlapText(place: AgePlace | null, reference: AgePlace): string | null {
  const weight = num(reference.overlap_weight);
  if (weight === null || weight >= 1) return null;
  const percent = Math.floor(weight * 1000) / 10;
  return `${formatNumber(percent, { maximumFractionDigits: 1 })}% of ${place?.geo_name || "this place"}'s area lies in ${reference.geo_name}`;
}

/** The toggle's choices: the API's references in its order, then None. */
export function referenceChoices(answer: AgeStructureResponse): ReferenceChoice[] {
  const place = placeOf(answer);
  const references = answer.places.filter((item) => item.role === "reference");
  return [
    ...references.map((reference) => ({
      key: reference.geo_id,
      levelWord: LEVEL_WORDS[reference.reference_level || reference.geo_level] || "Comparison",
      name: reference.geo_name,
      place: reference,
      overlapNote: overlapText(place, reference),
    })),
    ...(references.length ? [{ key: NO_REFERENCE, levelWord: "None", name: "", place: null, overlapNote: null }] : []),
  ];
}

/** The choice selected on load: the API's default (next level up), else the first reference. */
export function defaultReferenceKey(answer: AgeStructureResponse): string {
  const choices = referenceChoices(answer);
  const preferred = choices.find((choice) => choice.key === answer.default_reference_geo_id);
  return (preferred || choices[0])?.key || NO_REFERENCE;
}

export function referenceFor(answer: AgeStructureResponse, key: string): AgePlace | null {
  if (key === NO_REFERENCE) return null;
  return answer.places.find((item) => item.role === "reference" && item.geo_id === key) || null;
}

/** A place that can be drawn: it has bands, and was not refused. */
export function isDrawable(place: AgePlace | null): place is AgePlace {
  return Boolean(place && !place.unavailable_reason && place.bands.length);
}

// ---------------------------------------------------------------- the charts

/** The axis extent a set of pyramids share: the widest share plus its margin, rounded up to a whole percent. */
export function pyramidExtent(places: readonly (AgePlace | null)[]): number {
  let widest = 0;
  for (const place of places) {
    if (!isDrawable(place)) continue;
    for (const band of place.bands) {
      for (const sex of SEXES) {
        const share = num(band[sex].share);
        if (share === null) continue;
        widest = Math.max(widest, share + (num(band[sex].share_moe) || 0));
      }
    }
  }
  return Math.max(1, Math.ceil(widest));
}

export interface BandDifference {
  bandId: string;
  label: string;
  men: number | null;
  women: number | null;
}

/** Per band, the place's share minus the reference's, in percentage points; null when either side is unavailable. */
export function bandDifferences(place: AgePlace, reference: AgePlace): BandDifference[] {
  const byId = new Map(reference.bands.map((band) => [band.band_id, band]));
  return place.bands.map((band) => {
    const other = byId.get(band.band_id);
    const diff = (sex: Sex) => {
      const a = num(band[sex].share);
      const b = other ? num(other[sex].share) : null;
      return a === null || b === null ? null : a - b;
    };
    return { bandId: band.band_id, label: bandText(band), men: diff("men"), women: diff("women") };
  });
}

/** One sentence per bar, for keyboard focus and screen readers. */
export function bandSentence(place: AgePlace, band: AgeBand, sex: Sex, others: readonly AgePlace[] = []): string {
  const side = band[sex];
  const who = `${sexWord(sex, true)} aged ${bandText(band)}`;
  if (num(side.share) === null) {
    return `${who}: ${unavailableText(side.unavailable_reason)}.`;
  }
  const moe = num(side.count_moe);
  const reliability = side.reliable ? "" : " The survey's estimate for this group is unreliable: its margin of error is large next to the count.";
  const extras = others
    .map((other) => {
      const match = other.bands.find((item) => item.band_id === band.band_id);
      const share = match ? num(match[sex].share) : null;
      return share === null ? null : `${other.geo_name}, ${periodText(other)}: ${percentText(share)}.`;
    })
    .filter(Boolean)
    .join(" ");
  return (
    `${who}: ${percentText(side.share)} of ${place.geo_name}, ${countText(side.count)} people` +
    (moe === null ? "" : `, plus or minus ${countText(side.count_moe)}`) +
    `.${reliability}${extras ? ` ${extras}` : ""}`
  );
}

// ---------------------------------------------------------------- the cards

export interface AgeCardSpec {
  id: string;
  label: string;
  unit: string;
  /** The place's value, its margin and the comparison's value, as strings. */
  read: (place: AgePlace) => { value: string; moe: string; raw: number | null };
  /** Computed here under the scheme (draft-marked), or the provider's own figure. */
  derived: boolean;
  extra?: (place: AgePlace) => string | null;
}

const share = (key: keyof Pick<AgeIndicators, "under_18" | "age_65_plus" | "age_85_plus">) => (place: AgePlace) => {
  const item = place.indicators?.[key];
  return { value: percentText(item?.share ?? null), moe: marginText(item?.share_moe), raw: num(item?.share ?? null) };
};
const ratio = (key: keyof Pick<AgeIndicators, "child_dependency_ratio" | "old_age_dependency_ratio" | "total_dependency_ratio" | "sex_ratio">) =>
  (place: AgePlace) => {
    const item = place.indicators?.[key];
    return { value: decimalText(item?.value ?? null), moe: marginText(item?.moe), raw: num(item?.value ?? null) };
  };

export const AGE_CARDS: readonly AgeCardSpec[] = [
  {
    id: "median-age",
    label: "Median age",
    unit: "years",
    derived: false,
    read: (place) => ({ value: decimalText(place.median_age?.all.value ?? null), moe: marginText(place.median_age?.all.moe), raw: num(place.median_age?.all.value ?? null) }),
    extra: (place) =>
      place.median_age ? `Men ${decimalText(place.median_age.men.value)} · Women ${decimalText(place.median_age.women.value)}` : null,
  },
  { id: "under-18", label: "People under 18", unit: "of everyone", derived: true, read: share("under_18") },
  { id: "65-plus", label: "People 65 and over", unit: "of everyone", derived: true, read: share("age_65_plus") },
  { id: "85-plus", label: "People 85 and over", unit: "of everyone", derived: true, read: share("age_85_plus") },
  { id: "child-dependency", label: "Child dependency ratio", unit: "under 18 per 100 aged 18 to 64", derived: true, read: ratio("child_dependency_ratio") },
  { id: "old-age-dependency", label: "Old-age dependency ratio", unit: "65 and over per 100 aged 18 to 64", derived: true, read: ratio("old_age_dependency_ratio") },
  { id: "total-dependency", label: "Total dependency ratio", unit: "under 18 and 65 and over per 100 aged 18 to 64", derived: true, read: ratio("total_dependency_ratio") },
  { id: "sex-ratio", label: "Sex ratio", unit: "men per 100 women", derived: true, read: ratio("sex_ratio") },
];

// ---------------------------------------------------------------- history and cohorts

export const BROAD_GROUPS = [
  { key: "under_18", label: "Under 18" },
  { key: "age_18_64", label: "18 to 64" },
  { key: "age_65_plus", label: "65 and over" },
  { key: "age_85_plus", label: "85 and over" },
] as const;

/** The earlier releases an outline can be drawn from: every history item but the one shown, newest first. */
export function earlierReleases(history: AgeHistoryResponse, shownRelease: string | null): AgePlace[] {
  return history.items.filter((item) => item.release !== shownRelease && isDrawable(item));
}

/**
 * The release a cohort view starts from: five years before the one shown, so
 * each band moves up exactly one. For a five-year release that is also the
 * first whose period does not overlap it; the API refuses anything else.
 */
export function cohortFromRelease(release: string | null): string | null {
  const year = Number(release);
  if (!release || !Number.isInteger(year)) return null;
  return String(year - 5);
}

const REFUSAL_WORDS: Record<string, string> = {
  overlapping_periods: "The two survey periods share years, so the same responses would sit on both sides of the comparison.",
  period_gap_not_five_years: "The two survey periods do not end five years apart, so an age group cannot be followed into the next one.",
  boundary_changed: "This place's boundary was redrawn between the two surveys, so the counts describe different areas.",
  not_published: "One of the two surveys is not published for this place.",
};

export function cohortRefusalText(refusal: CohortRefusal): string {
  return REFUSAL_WORDS[refusal.reason] || "These two surveys cannot be compared for this place.";
}

/**
 * One cohort sentence, within ADR-0011's verbs: "rose", "fell", or "was not
 * distinguishable from" when the two 90 % intervals overlap.
 */
export function cohortSentence(item: CohortItem, sex: Sex, from: CohortPeriod, to: CohortPeriod): string {
  const side = item[sex];
  const fromText = `${sexWord(sex, true)} aged ${bandIdText(item.from_band)} in ${periodText(from)}`;
  const fromLower = `${sexWord(sex)} aged ${bandIdText(item.from_band)} in ${periodText(from)}`;
  const toText = `${sexWord(sex)} aged ${bandIdText(item.to_band)} in ${periodText(to)}`;
  if (num(side.from_count) === null || num(side.to_count) === null) {
    return `${fromText}, or ${toText}: not available.`;
  }
  const change = num(side.change);
  const counts = `${countText(side.from_count)} then ${countText(side.to_count)}`;
  // Only intervals the API says do not overlap get "rose" or "fell"
  // (ADR-0011 rule 3); unknown is not distinguishable.
  if (side.intervals_overlap !== false || change === null) {
    return `${fromText} numbered ${countText(side.from_count)}; ${toText}, ${countText(side.to_count)}, which was not distinguishable from it.`;
  }
  const verb = change > 0 ? "rose" : change < 0 ? "fell" : "was unchanged";
  return `From ${fromLower} to ${toText}, the count ${verb} (${counts}, a change of ${signedCount(change)} ${marginText(side.change_moe, 0)}).`;
}

function signedCount(value: number): string {
  const sign = value > 0 ? "+" : value < 0 ? "−" : "";
  return `${sign}${formatNumber(Math.abs(value), { maximumFractionDigits: 0 })}`;
}

/** The lines of a band, for the provenance record only (never shown as text elsewhere). */
export function bandLines(band: AgeBand): string {
  return [...band.lines.men, ...band.lines.women].join(", ");
}
