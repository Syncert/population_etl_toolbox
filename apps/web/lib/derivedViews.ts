// Per-capita and share views of a count (web-per-capita-toggle, ADR-0009).
//
// The API owns every rule here: which count pairs with which denominator,
// the scale, the rounding, the margin of error and why a value is missing.
// This module only chooses among the pairings the catalog serves and words
// what the API returned, so the denominator is named beside every value.

import { apiFetch } from "./api/client";
import type { QueryParams, RequestOptions } from "./api/client";
import { formatObservationValue } from "./explorerViewModel";
import { formatNumber } from "./format";
import type { ObservationRow } from "./explorerViewModel";
import { observationPeriodLabel } from "./observationAccess";

export type DerivedKind = "share" | "per_capita" | "provider_rate" | "agency_rate";
export type PairingStatus = "approved" | "draft" | "needs_review" | "deprecated";
export type MarginMethod = "proportion" | "ratio" | "ratio_fallback" | "unavailable" | "provider" | "none";
export type UnavailableReason =
  | "different_period"
  | "denominator_missing"
  | "denominator_zero"
  | "suppressed_input"
  | "no_reporting_agency"
  | "only_multi_county_agencies"
  | "coverage_below_floor";

/** One pairing as `GET /api/v1/catalog/derived-ratios` lists it. */
export interface DerivedRatioPairing {
  ratio_id: string;
  kind: DerivedKind;
  derived: boolean;
  label: string;
  unit: string;
  scale: number;
  numerator: string;
  denominator?: string | null;
  provider_rate?: string | null;
  /** `agency_rate` only: the provider rate it stands beside, and its levels. */
  reads_beside?: string | null;
  geo_levels?: string[];
  dataset?: string | null;
  denominator_label: string;
  universe: string;
  numerator_is_subset?: boolean;
  status: PairingStatus;
  version: number;
  served: boolean;
  refusal_reason?: string | null;
  limitations?: string[];
}

export interface DerivedRatioInput {
  metric_code: string;
  metric_label?: string | null;
  release?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  value?: string | null;
  value_status?: string | null;
  margin_of_error?: string | null;
  margin_of_error_controlled?: boolean;
}

/** Whom a county agency rate covers (ADR-0012); every figure is the FBI's. */
export interface DerivedRatioCoverage {
  covered_population?: string | null;
  linked_population?: string | null;
  coverage_share?: string | null;
  coverage_floor: string;
  contributing_agencies: string[];
  excluded_multi_county_agencies: string[];
  non_reporting_agencies: string[];
  unknown_population_agencies: string[];
}

/** One row of `GET /api/v1/observations/derived`. */
export interface DerivedRatioObservation {
  ratio_id: string;
  derived: boolean;
  pairing_status: PairingStatus;
  pairing_version: number;
  geo_id?: string | null;
  geo_level?: string | null;
  geo_name?: string | null;
  period_start?: string | null;
  period_end?: string | null;
  release?: string | null;
  value?: string | null;
  unit: string;
  scale: number;
  margin_of_error?: string | null;
  margin_of_error_method: MarginMethod;
  unavailable_reason?: UnavailableReason | null;
  numerator?: DerivedRatioInput | null;
  denominator?: DerivedRatioInput | null;
  provider_rate?: DerivedRatioInput | null;
  coverage?: DerivedRatioCoverage | null;
}

export interface DerivedRatioPage {
  ratio: DerivedRatioPairing;
  total: number;
  limit: number;
  offset: number;
  items: DerivedRatioObservation[];
}

/** What a reader can switch a count to. Provider rates are a per-capita view. */
export type ViewChoice = "count" | "per_capita" | "share";

export const VIEW_CHOICES: readonly ViewChoice[] = ["count", "per_capita", "share"];

export function choiceOf(kind: DerivedKind): Exclude<ViewChoice, "count"> {
  return kind === "share" ? "share" : "per_capita";
}

/** The served pairing that divides this count in the chosen way, if any. */
export function pairingFor(
  pairings: readonly DerivedRatioPairing[],
  metricCode: string,
  choice: ViewChoice,
): DerivedRatioPairing | null {
  if (choice === "count") return null;
  // A county agency rate is not a view of the count: it stands in the rate
  // card's county row, where the FBI publishes none (ADR-0012).
  return pairings.find((pairing) => pairing.served && pairing.kind !== "agency_rate" && pairing.numerator === metricCode && choiceOf(pairing.kind) === choice) || null;
}

/** The served county rate computed from agency reports that stands beside this FBI rate, if any. */
export function agencyRateFor(pairings: readonly DerivedRatioPairing[], metricCode: string): DerivedRatioPairing | null {
  return pairings.find((pairing) => pairing.served && pairing.kind === "agency_rate" && pairing.reads_beside === metricCode) || null;
}

/** The provider's own rate this metric is, if a pairing names it as one. */
export function providerRateFor(pairings: readonly DerivedRatioPairing[], metricCode: string): DerivedRatioPairing | null {
  return pairings.find((pairing) => pairing.kind === "provider_rate" && pairing.provider_rate === metricCode) || null;
}

/** The views any of these counts offers, "count" first; only "count" means no toggle. */
export function viewChoices(pairings: readonly DerivedRatioPairing[], metricCodes: readonly string[]): ViewChoice[] {
  return VIEW_CHOICES.filter((choice) => choice === "count" || metricCodes.some((code) => pairingFor(pairings, code, choice)));
}

/**
 * The toggle's words for a choice. A per-capita choice takes its unit from
 * the pairings it would show when they agree ("Per 1,000 residents"), so the
 * button never promises a scale the API does not serve.
 */
export function choiceLabel(choice: ViewChoice, pairings: readonly DerivedRatioPairing[] = []): string {
  if (choice === "count") return "Count";
  if (choice === "share") return "Share";
  const units = [...new Set(pairings.filter((pairing) => choiceOf(pairing.kind) === "per_capita").map((pairing) => pairing.unit))];
  return units.length === 1 ? sentenceCase(units[0]!) : "Per resident";
}

function sentenceCase(text: string): string {
  return text ? text[0]!.toUpperCase() + text.slice(1) : text;
}

export function isDraft(status: PairingStatus | string | null | undefined): boolean {
  return status !== "approved";
}

/** D-2 marks a draft label "Plain label awaiting review"; a draft pairing reads alike. */
export const DRAFT_VIEW_TEXT = "Draft view awaiting review";

export function draftViewNote(pairing: Pick<DerivedRatioPairing, "status" | "version">): string {
  return `This pairing of a count and its denominator is ${pairing.status.replace("_", " ")} (version ${pairing.version}) and has not been reviewed yet.`;
}

/** The denominator, as it is named beside a value. */
export function denominatorText(pairing: DerivedRatioPairing): string {
  if (pairing.kind === "share") return `of ${pairing.denominator_label}`;
  if (pairing.kind === "provider_rate") return `denominator: ${pairing.denominator_label}, as the publisher computed it`;
  return `denominator: ${pairing.denominator_label}`;
}

const UNAVAILABLE_TEXT: Record<UnavailableReason, string> = {
  different_period: "No denominator for this period and release",
  denominator_missing: "The denominator is not published here",
  denominator_zero: "The denominator is zero",
  suppressed_input: "An input is withheld or not a usable number",
  no_reporting_agency: "No agency serving this county reported this month",
  only_multi_county_agencies: "Only agencies that also serve other counties reported",
  coverage_below_floor: "Too few agencies reported to give a rate",
};

export function unavailableText(reason: string | null | undefined): string {
  return (reason && UNAVAILABLE_TEXT[reason as UnavailableReason]) || "No value";
}

/**
 * Whom a county agency rate covers, in the words ADR-0012 rule 3 puts beside
 * the figure: "covers 115,648 people, 100 % of the population its agencies
 * serve". A refused month says the same, so the reason is visible.
 */
export function coverageText(coverage: DerivedRatioCoverage | null | undefined): string {
  if (!coverage) return "";
  const covered = Number(coverage.covered_population);
  const share = Number(coverage.coverage_share);
  if (coverage.covered_population === null || coverage.covered_population === undefined || !Number.isFinite(covered)) {
    return "no reporting agency serving only this county published a population";
  }
  const people = `covers ${formatNumber(covered, { maximumFractionDigits: 0 })} ${covered === 1 ? "person" : "people"}`;
  if (coverage.coverage_share === null || coverage.coverage_share === undefined || !Number.isFinite(share)) return people;
  return `${people}, ${formatNumber(Math.floor(share * 100), { maximumFractionDigits: 0 })} % of the population its agencies serve`;
}

/** The agencies behind a county rate, for its tooltip and the reader who looks. */
export function agenciesText(coverage: DerivedRatioCoverage | null | undefined): string {
  if (!coverage) return "";
  const count = (items: string[], one: string, many: string) => (items.length ? `${items.length} ${items.length === 1 ? one : many}` : "");
  return [
    count(coverage.contributing_agencies, "reporting agency", "reporting agencies"),
    count(coverage.non_reporting_agencies, "agency that did not report", "agencies that did not report"),
    count(coverage.excluded_multi_county_agencies, "agency left out because it also serves other counties", "agencies left out because they also serve other counties"),
    count(coverage.unknown_population_agencies, "agency left out with no published population", "agencies left out with no published population"),
  ].filter(Boolean).join("; ");
}

const MARGIN_METHOD_TEXT: Record<MarginMethod, string> = {
  proportion: "Census Bureau formula for a derived proportion",
  ratio: "Census Bureau formula for a derived ratio",
  ratio_fallback: "Census Bureau ratio formula, used where the proportion formula does not apply",
  unavailable: "an input's margin is not published",
  provider: "the publisher's own margin",
  none: "",
};

/** The value with its unit, or the API's reason there is none. Never zero for missing. */
export function derivedValueText(row: DerivedRatioObservation): string {
  if (row.value === null || row.value === undefined || row.unavailable_reason) return unavailableText(row.unavailable_reason);
  return `${formatObservationValue(row.value, 2)} ${row.unit}`.trim();
}

export function derivedMarginText(row: DerivedRatioObservation): string {
  if (row.value === null || row.value === undefined) return "";
  const method = MARGIN_METHOD_TEXT[row.margin_of_error_method] || "";
  if (row.margin_of_error !== null && row.margin_of_error !== undefined && row.margin_of_error !== "") {
    return `margin of error ±${formatObservationValue(row.margin_of_error, 2)}${method ? ` (${method})` : ""}`;
  }
  return row.margin_of_error_method === "unavailable" ? `margin of error not computed: ${method}` : "";
}

/** The period a derived row describes, from the row or its inputs. */
export function derivedPeriodText(row: DerivedRatioObservation): string {
  return observationPeriodLabel(row as unknown as ObservationRow)
    || observationPeriodLabel((row.numerator || row.provider_rate || {}) as ObservationRow);
}

/**
 * A derived row in the shape the county map, legend and tables read. Its
 * `metric_code` is the view's key, so the map's one-measure check holds; a
 * row with no value carries the API's reason as its status.
 */
export function derivedMapRow(row: DerivedRatioObservation, viewKey: string): ObservationRow {
  return {
    metric_code: viewKey,
    geo_id: row.geo_id,
    geo_level: row.geo_level,
    geo_name: row.geo_name,
    period_start: row.period_start ?? row.numerator?.period_start ?? row.provider_rate?.period_start ?? null,
    period_end: row.period_end ?? row.numerator?.period_end ?? row.provider_rate?.period_end ?? null,
    release: row.release,
    value: row.unavailable_reason ? null : row.value ?? null,
    value_status: row.unavailable_reason ? unavailableText(row.unavailable_reason) : "valid",
    unit: row.unit,
    margin_of_error: row.margin_of_error ?? null,
    derived: row.derived,
    pairing_status: row.pairing_status,
  };
}

export function viewKey(pairing: DerivedRatioPairing): string {
  return `derived:${pairing.ratio_id}`;
}

/** The per-capita and share pairings the API lists. */
export function getDerivedRatios(
  params: QueryParams = {},
  options: RequestOptions = {},
): Promise<{ total: number; items: DerivedRatioPairing[] }> {
  return apiFetch<{ total: number; items: DerivedRatioPairing[] }>("/catalog/derived-ratios", { ...options, params });
}

/** One pairing's values, computed by the API over served rows. */
export function getDerivedObservations(params: QueryParams, options: RequestOptions = {}): Promise<DerivedRatioPage> {
  return apiFetch<DerivedRatioPage>("/observations/derived", { ...options, params });
}
