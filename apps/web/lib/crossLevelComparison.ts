// One measure in two places at any level (web-cross-level-compare, ADR-0010).
//
// `/api/v1/comparison/places` decides everything: whether the two values may
// be set side by side, which period each shows, how the places relate, and
// whether a difference may be computed. This module asks it and words the
// answer for a reader. The API's own `detail` strings name codes and sides
// ("side a"), so every sentence here is written from the reason and the two
// places' names instead, and the detail is kept only in hover text.

import { apiFetch } from "./api/client";
import type { RequestOptions } from "./api/client";
import { formatObservationValue, publishedNumber } from "./explorerViewModel";
import type { ObservationRow } from "./explorerViewModel";
import { observationPeriodLabel } from "./observationAccess";
import type { PlaceLevel } from "./placeChapters";

/** The grains a comparison side can be: the place pages' plus the metro. */
export type CompareLevel = PlaceLevel | "METRO";

export type MeasureKind = "count" | "median" | "mean" | "rate" | "share" | "index";
export type RefusalReason = "counts_differ_in_size" | "programs_differ" | "universes_differ" | "units_differ" | "different_measures";
export type NoteReason =
  | "periods_differ"
  | "not_published"
  | "contained"
  | "overlapping_places"
  | "intervals_overlap"
  | "no_margin_published"
  | "counts_differ_in_size";
export type Relationship = "contains" | "part_of" | "intersects" | "adjacent" | "none_recorded";

export interface ComparisonAlternative {
  ratio_id: string;
  label: string;
  unit: string;
  href: string;
}

export interface PlacesSide {
  geo_id: string;
  geo_level?: string | null;
  geo_name?: string | null;
  metric_code: string;
  published?: boolean | null;
  period_start?: string | null;
  period_end?: string | null;
  release?: string | null;
  value?: string | null;
  value_status?: string | null;
  margin_of_error?: string | null;
  interval_low?: string | null;
  interval_high?: string | null;
}

export interface PlacesComparison {
  metric_code?: string | null;
  ratio_id?: string | null;
  measure_kind: MeasureKind;
  measure_kind_status: string;
  cross_level: boolean;
  comparable: boolean;
  refusal?: { reason: RefusalReason; detail: string; alternatives?: ComparisonAlternative[] } | null;
  period_basis?: "requested" | "newest_common" | "each_newest" | null;
  side_a: PlacesSide;
  side_b: PlacesSide;
  containment: {
    relationship: Relationship;
    smaller_side?: "a" | "b" | null;
    share_of_smaller_inside_larger?: number | null;
  };
  difference?: string | null;
  intervals_overlap?: boolean | null;
  derived: boolean;
  notes: { reason: NoteReason; detail: string }[];
  caveats: string[];
}

export interface ComparedSide {
  geoId: string;
  name: string;
  level: CompareLevel;
}

export function getPlacesComparison(
  params: { metric_code?: string; ratio_id?: string; geo_id_a: string; geo_id_b: string },
  options: RequestOptions = {},
): Promise<PlacesComparison> {
  return apiFetch<PlacesComparison>("/comparison/places", { ...options, params });
}

const LEVEL_WORDS: Record<CompareLevel, string> = {
  NATIONAL: "the nation",
  STATE: "a state",
  COUNTY: "a county",
  PLACE: "a city or town",
  METRO: "a metro area",
};

/** "a county", "the nation": how a place's level reads in a sentence. */
export function levelWords(level: CompareLevel | string): string {
  return LEVEL_WORDS[level as CompareLevel] || "a place";
}

/** Whether two sides sit at different levels, which the page then says. */
export function isCrossLevel(a: ComparedSide, b: ComparedSide): boolean {
  return a.level !== b.level;
}

/**
 * Why the two values are not set side by side, in a reader's words. A count
 * across levels says what it is offered instead when the API serves one.
 */
export function refusalSentence(answer: PlacesComparison, a: ComparedSide, b: ComparedSide): string {
  const reason = answer.refusal?.reason;
  if (reason === "counts_differ_in_size") {
    const sizes = a.level === b.level
      ? `${a.name} and ${b.name} differ in size`
      : `${a.name} is ${levelWords(a.level)} and ${b.name} is ${levelWords(b.level)}, so they differ in size by their nature`;
    const offer = answer.refusal?.alternatives?.length
      ? " It is shown as a share or per resident instead, which puts both places on one footing."
      : " No share or per-resident version of this total is published yet, so it is left out.";
    return `This is a total, and ${sizes}: the larger place nearly always has the larger total, which says nothing about either one.${offer}`;
  }
  if (reason === "programs_differ") return "The two figures come from different surveys or programs that measure this in different ways, so they are not set side by side.";
  if (reason === "universes_differ") return "The two figures count different groups of people, so they are not set side by side.";
  if (reason === "units_differ") return "The two figures are in different units, so they are not set side by side.";
  if (reason === "different_measures") return "These are two different measures, so they are not set side by side.";
  return "This measure cannot be compared between these two places.";
}

function percent(share: number | null | undefined): string {
  return typeof share === "number" && Number.isFinite(share) ? `${Math.round(share * 100)}%` : "";
}

/**
 * How the places relate, when one sits inside the other or they overlap.
 * Either way the two figures partly count the same people, so no difference
 * is worked out; the sentence says so.
 */
export function containmentSentence(answer: PlacesComparison, a: ComparedSide, b: ComparedSide): string {
  const { relationship, smaller_side: smaller, share_of_smaller_inside_larger: share } = answer.containment;
  if (relationship === "contains" || relationship === "part_of") {
    const inner = relationship === "part_of" ? a : b;
    const outer = inner === a ? b : a;
    return `${inner.name} lies inside ${outer.name}, so ${outer.name}'s figure already includes ${inner.name}. They are shown side by side; no difference is worked out between them.`;
  }
  if (relationship === "intersects") {
    const small = smaller === "b" ? b : a;
    const large = small === a ? b : a;
    const part = percent(share);
    return `${part ? `${part} of ${small.name}'s area` : `Part of ${small.name}`} lies inside ${large.name}, so the two figures partly describe the same people. They are shown side by side; no difference is worked out between them.`;
  }
  return "";
}

/** The limits the API names beside a served comparison, as sentences. */
export function noteSentences(answer: PlacesComparison, a: ComparedSide, b: ComparedSide): string[] {
  const sentences: string[] = [];
  for (const note of answer.notes || []) {
    if (note.reason === "periods_differ") {
      sentences.push(`No single period covers both: ${a.name}'s newest figure is for ${sidePeriod(answer.side_a)} and ${b.name}'s is for ${sidePeriod(answer.side_b)}. No difference is worked out across different periods.`);
    } else if (note.reason === "intervals_overlap") {
      sentences.push("The two margins of error overlap, so these places may not truly differ on this measure.");
    } else if (note.reason === "counts_differ_in_size") {
      sentences.push("These are totals, and the two places differ in size; a share or per-resident view compares like with like.");
    }
    // `not_published` is said beside the side itself; `contained` and
    // `overlapping_places` by the containment sentence; a missing margin
    // is silence, not a claim either way.
  }
  return [...new Set(sentences)];
}

export function sidePeriod(side: PlacesSide): string {
  return observationPeriodLabel(side as unknown as ObservationRow);
}

/** A side's value, or that it has none. A missing value is never zero. */
export function sideValue(side: PlacesSide): number | null {
  return side.published === false ? null : publishedNumber(side.value);
}

export function sideValueText(side: PlacesSide, unit: string, digits = 1): string {
  const value = sideValue(side);
  if (value === null) {
    return side.value_status && side.value_status !== "valid" && side.published !== false
      ? "Withheld by the publisher"
      : "Not published for this place";
  }
  return `${formatObservationValue(value, digits)} ${unit}`.trim();
}

export function sideMarginText(side: PlacesSide, digits = 1): string {
  const margin = publishedNumber(side.margin_of_error);
  if (margin === null || margin < 0 || sideValue(side) === null) return "";
  return `±${formatObservationValue(margin, digits)}`;
}

/** Where a value sits on a bar shared by the two sides, 0-100, from zero. */
export function sideBar(value: number, answer: PlacesComparison): number {
  const values = [sideValue(answer.side_a), sideValue(answer.side_b), 0].filter((item): item is number => item !== null);
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (max === min) return 100;
  return Math.min(100, Math.max(0, ((value - min) / (max - min)) * 100));
}

/** The API's difference, a minus b, only when it computed one. */
export function differenceText(answer: PlacesComparison, a: ComparedSide, b: ComparedSide, unit: string, digits = 1): string {
  const difference = publishedNumber(answer.difference);
  if (difference === null) return "";
  return `${a.name} minus ${b.name}: ${formatObservationValue(difference, digits)} ${unit}`.trim();
}
