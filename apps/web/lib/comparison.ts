// Comparison workspace view models.
//
// The governing rule of this module is that the API owns the compatibility
// decision and this client only presents it. `/comparison/preflight` returns
// a three-valued verdict per declared rule over published semantics, and
// `/comparison` enforces exactly that verdict. So nothing here may decide a
// pair is comparable, re-rank the rules, treat an `unknown` as a pass, or
// build a comparison request for a pair the preflight blocked — the point of
// asking first is that no incompatible data ever moves.
//
// The second rule is that provider-published inputs and API-derived
// combinations never blur together. Each row carries both sides' own value,
// period, and identity; `difference` and `ratio` are computed by the API and
// are labelled as derived wherever they appear, including in the export.

import type {
  ComparisonPreflight,
  ComparisonResponse,
  ComparisonRow,
  ComparisonRule,
  MetricSummary,
} from "./api/types";
import { metricSupportedGeoLevels, normalizeGeoLevel } from "./explorerViewModel";
import type { ObservationRow } from "./explorerViewModel";
import { GEO_GRAIN_LABELS, GEO_GRAIN_ORDER } from "./geographyPicker";
import { formatNumber } from "./format";

export const RULE_PASS = "pass";
export const RULE_FAIL = "fail";
export const RULE_UNKNOWN = "unknown";

/** One side of a comparison, as the user selected it. */
export interface ComparisonSide {
  sourceCode: string;
  metricCode: string;
}

export interface ComparisonSelection {
  a: ComparisonSide;
  b: ComparisonSide;
  geoLevel: string;
  stateFips: string;
}

export const DEFAULT_COMPARISON_SELECTION: ComparisonSelection = Object.freeze({
  a: Object.freeze({ sourceCode: "", metricCode: "" }) as ComparisonSide,
  b: Object.freeze({ sourceCode: "", metricCode: "" }) as ComparisonSide,
  geoLevel: "COUNTY",
  stateFips: "",
});

/** True once both sides name a measure, so a preflight can be asked for. */
export function selectionIsComplete(selection: ComparisonSelection): boolean {
  return Boolean(selection.a.metricCode && selection.b.metricCode);
}

export interface PreflightModel {
  /** The API's verdict. Never computed here. */
  comparable: boolean;
  /** Rules that positively failed — the reasons the pair is blocked. */
  blocking: ComparisonRule[];
  /**
   * Rules the publication left unverifiable. These do not block: where a
   * source publishes nothing to check, the comparison is served and the
   * unverified rule travels as a caveat.
   */
  unverified: ComparisonRule[];
  passed: ComparisonRule[];
  /** Fields `/comparison` computes; every one is API-derived, not published. */
  derivations: string[];
  caveats: string[];
}

const EMPTY_PREFLIGHT: PreflightModel = Object.freeze({
  comparable: false,
  blocking: [],
  unverified: [],
  passed: [],
  derivations: [],
  caveats: [],
}) as PreflightModel;

function rulesOf(preflight: ComparisonPreflight | null | undefined): ComparisonRule[] {
  return Array.isArray(preflight?.rules) ? preflight.rules : [];
}

/**
 * Split the published verdict into the three groups a reader needs, without
 * reinterpreting any of them. `comparable` is read from the response rather
 * than inferred from the rule list, so a future rule this client has never
 * heard of cannot flip the decision.
 */
export function describePreflight(
  preflight: ComparisonPreflight | null | undefined,
): PreflightModel {
  if (!preflight) {
    return EMPTY_PREFLIGHT;
  }
  const rules = rulesOf(preflight);
  return {
    comparable: preflight.comparable === true,
    blocking: rules.filter((rule) => rule.status === RULE_FAIL),
    unverified: rules.filter((rule) => rule.status === RULE_UNKNOWN),
    passed: rules.filter((rule) => rule.status === RULE_PASS),
    derivations: Array.isArray(preflight.derivations) ? preflight.derivations : [],
    caveats: Array.isArray(preflight.caveats) ? preflight.caveats : [],
  };
}

export interface CompatibilityState {
  /** Shared request-state vocabulary value, for the status pill. */
  state: string;
  message: string;
}

/**
 * The verdict as a status-pill state.
 *
 * A comparable pair with nothing left unverified is the only `ok`. A
 * comparable pair carrying unverified rules is a caution, because something
 * the comparison depends on could not be checked. A blocked pair is
 * `incompatible` — a failure-shaped state, so it can never read as healthy.
 */
export function compatibilityState(
  preflight: ComparisonPreflight | null | undefined,
): CompatibilityState {
  if (!preflight) {
    return { state: "idle", message: "select two measures" };
  }
  const model = describePreflight(preflight);
  if (!model.comparable) {
    const count = model.blocking.length;
    return {
      state: "incompatible",
      message: `not comparable: ${count} declared rule${count === 1 ? "" : "s"} failed`,
    };
  }
  if (model.unverified.length > 0) {
    return {
      state: "warn",
      message: `comparable; ${model.unverified.length} rule${
        model.unverified.length === 1 ? "" : "s"
      } could not be verified`,
    };
  }
  return { state: "ok", message: "comparable; every declared rule passed" };
}

/**
 * Whether a comparison request may be issued.
 *
 * `/comparison` answers an incompatible pair with a 422, so asking anyway
 * would turn a stated explanation into a request failure — and would move
 * data for a pair the policy rejected.
 */
export function mayRequestComparison(
  preflight: ComparisonPreflight | null | undefined,
): boolean {
  return describePreflight(preflight).comparable;
}

/**
 * What a reader can do instead when a pair is blocked.
 *
 * These are navigational alternatives derived from the failed rules — each
 * measure remains fully explorable on its own — never a weakened comparison
 * or a suggestion to compare something the policy declined.
 */
export function incompatibleAlternatives(
  preflight: ComparisonPreflight | null | undefined,
): string[] {
  const model = describePreflight(preflight);
  if (model.comparable || model.blocking.length === 0) {
    return [];
  }

  const alternatives = [
    "Explore each measure on its own, where its published values, periods, and caveats stay intact.",
  ];
  const failedRules = new Set(model.blocking.map((rule) => rule.rule));
  if (failedRules.has("source_analysis_ready")) {
    alternatives.push(
      "One side's source publishes stratified, multi-dimensional, or agency-grain observations that an aligned one-value-per-geography comparison would collapse. Query it through the explorer with its own declared filters instead.",
    );
  }
  if (failedRules.has("units")) {
    alternatives.push(
      "Choose measures the publishers state in the same unit, or read each unit's own series separately.",
    );
  }
  if (failedRules.has("time_grains")) {
    alternatives.push(
      "Choose measures published on a shared time grain; aligning different grains would invent a period neither publisher stated.",
    );
  }
  if (failedRules.has("geo_grains")) {
    alternatives.push(
      "Choose measures published for a shared geography grain, or compare at a grain both publish.",
    );
  }
  return alternatives;
}

/** Parameters `/comparison/preflight` declares. */
export function preflightRequestParams(selection: ComparisonSelection): Record<string, string> {
  return {
    metric_code_a: selection.a.metricCode,
    metric_code_b: selection.b.metricCode,
  };
}

/**
 * Parameters `/comparison` declares. `state_fips` is dropped at the national
 * grain, where scoping to one state would contradict the selection rather
 * than narrow it.
 */
export function comparisonRequestParams(
  selection: ComparisonSelection,
  limit: number | string = 1000,
): Record<string, string> {
  const params: Record<string, string> = {
    metric_code_a: selection.a.metricCode,
    metric_code_b: selection.b.metricCode,
    limit: String(limit),
  };
  if (selection.geoLevel) {
    params.geo_level = selection.geoLevel;
  }
  if (selection.stateFips && selection.geoLevel !== "NATIONAL") {
    params.state_fips = selection.stateFips;
  }
  return params;
}

/** A geography's published name, from the row's own attribution fields. */
export function comparisonRowName(row: ComparisonRow | null | undefined): string {
  const county = row?.county_name;
  const state = row?.state_name;
  if (county && state) {
    return `${county}, ${state}`;
  }
  return String(county || state || row?.geo_id || "");
}

/**
 * A published input value for display. `null` means that side published
 * nothing for this geography, which is reported as such — never as zero, and
 * never as a value borrowed from the other side.
 */
export function comparisonValueText(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) {
    return "Not published";
  }
  return formatNumber(value, { maximumFractionDigits: 4 });
}

/** True when the API named this field as one it derived. */
export function isDerivedField(
  response: ComparisonResponse | ComparisonPreflight | null | undefined,
  field: string,
): boolean {
  const derivations = Array.isArray(response?.derivations) ? response.derivations : [];
  return derivations.includes(field);
}

export interface ComparisonColumn {
  key: string;
  label: string;
  /** True for API-computed columns, so they are never read as published. */
  derived: boolean;
}

/**
 * The table columns, with each side's identity preserved in its own header
 * and every API-computed column marked derived. The derived columns are the
 * ones the response names, so a derivation this client has not heard of
 * still appears — labelled — instead of being dropped.
 */
export function comparisonColumns(
  response: ComparisonResponse | null | undefined,
): ComparisonColumn[] {
  const codeA = response?.metric_code_a || "measure A";
  const codeB = response?.metric_code_b || "measure B";
  const derivations = Array.isArray(response?.derivations) ? response.derivations : [];
  return [
    { key: "geography", label: "Geography", derived: false },
    { key: "value_a", label: codeA, derived: false },
    { key: "period_a", label: `${codeA} period`, derived: false },
    { key: "value_b", label: codeB, derived: false },
    { key: "period_b", label: `${codeB} period`, derived: false },
    ...derivations.map((field) => ({
      key: field,
      label: field,
      derived: true,
    })),
  ];
}

/** One row's cell values, keyed by the column keys above. */
export function comparisonCells(
  response: ComparisonResponse | null | undefined,
  row: ComparisonRow,
): Record<string, string> {
  const cells: Record<string, string> = {
    geography: comparisonRowName(row),
    value_a: comparisonValueText(row.value_a),
    period_a: String(row.period_a || "Not published"),
    value_b: comparisonValueText(row.value_b),
    period_b: String(row.period_b || "Not published"),
  };
  for (const field of Array.isArray(response?.derivations) ? response.derivations : []) {
    cells[field] = comparisonValueText(row[field] as number | null | undefined);
  }
  return cells;
}

/**
 * True when the two sides' published periods differ for this geography.
 *
 * The API combines each side's own newest value rather than aligning them to
 * a shared period, so a differing as-of context is a real property of the
 * row. Marking it keeps the reader from taking the pair as contemporaneous.
 */
export function periodsDiffer(row: ComparisonRow | null | undefined): boolean {
  const a = row?.period_a;
  const b = row?.period_b;
  return Boolean(a && b && String(a) !== String(b));
}

export interface ComparisonExport {
  headings: string[];
  rows: string[][];
  filename: string;
}

/**
 * How much of the aligned answer a file holds.
 *
 * The workspace pages `/comparison` and computes this to say "loaded 8,000
 * of 12,400 aligned geographies; the page bound cut the answer short" in its
 * status pill. The file outlives the pill, so it travels with the export
 * (WEB-067) -- the argument WEB-059 makes for the explorer's own file.
 */
export interface ComparisonLoad {
  loaded: number;
  total?: number | null;
  complete: boolean;
}

/**
 * The export carries its own interpretation envelope: both measures and
 * their sources and units, each row's own published values and periods, the
 * derived fields marked as derived in the heading itself, and every caveat
 * the verdict published — so the file can be read outside this application
 * without losing what the API said about it.
 */
export function comparisonExport(
  response: ComparisonResponse | null | undefined,
  preflight: ComparisonPreflight | null | undefined,
  load: ComparisonLoad | null = null,
): ComparisonExport {
  const items = Array.isArray(response?.items) ? response.items : [];
  const derivations = Array.isArray(response?.derivations) ? response.derivations : [];
  const codeA = response?.metric_code_a || "";
  const codeB = response?.metric_code_b || "";
  // A bounded read is the first thing a reader of this file needs to know,
  // so it leads the caveats rather than trailing the API's own.
  const shortfall =
    load && !load.complete
      ? [
          typeof load.total === "number" && Number.isFinite(load.total)
            ? `incomplete: ${load.loaded} of ${load.total} aligned geographies; ` +
              "the page bound cut the answer short"
            : `incomplete: ${load.loaded} aligned geographies loaded and no ` +
              "total published, so whether more exist is unknown",
        ]
      : [];
  const caveats = [
    ...shortfall,
    ...(Array.isArray(response?.caveats) ? response.caveats : []),
    ...describePreflight(preflight).unverified.map(
      (rule) => `unverified ${rule.rule}: ${rule.reason}`,
    ),
  ];

  const headings = [
    "geo_id",
    "geo_name",
    "geo_level",
    "metric_code_a",
    "source_code_a",
    "units_a",
    "period_a",
    "value_a",
    "metric_code_b",
    "source_code_b",
    "units_b",
    "period_b",
    "value_b",
    ...derivations.map((field) => `${field} (API-derived)`),
    "caveats",
  ];

  const caveatText = caveats.join(" | ");
  const rows = items.map((row) => [
    String(row.geo_id ?? ""),
    comparisonRowName(row),
    String(row.geo_level ?? ""),
    String(row.metric_code_a ?? codeA),
    String(response?.source_code_a ?? ""),
    String(response?.units_a ?? ""),
    String(row.period_a ?? ""),
    row.value_a === null || row.value_a === undefined ? "" : String(row.value_a),
    String(row.metric_code_b ?? codeB),
    String(response?.source_code_b ?? ""),
    String(response?.units_b ?? ""),
    String(row.period_b ?? ""),
    row.value_b === null || row.value_b === undefined ? "" : String(row.value_b),
    ...derivations.map((field) => {
      const value = row[field];
      return value === null || value === undefined ? "" : String(value);
    }),
    caveatText,
  ]);

  const slug = (code: string) => code.replaceAll(":", "-") || "measure";
  const stem = `comparison-${slug(codeA)}-vs-${slug(codeB)}`;
  // Named the way the explorer's partial file is (WEB-059): a complete read
  // keeps the name it always had.
  const of =
    load && typeof load.total === "number" && Number.isFinite(load.total)
      ? `-of-${load.total}`
      : "";
  return {
    headings,
    rows,
    filename:
      load && !load.complete
        ? `${stem}-partial-${load.loaded}${of}.csv`
        : `${stem}.csv`,
  };
}

/** Measure options for one side's picker, from the published catalog. */
export function comparisonMetricOptions(
  metrics: MetricSummary[] | null | undefined,
): { value: string; label: string }[] {
  return (Array.isArray(metrics) ? metrics : []).map((metric) => ({
    value: metric.metric_code,
    label: metric.metric_display_name
      ? `${metric.metric_display_name} (${metric.metric_code})`
      : metric.metric_code,
  }));
}

// --- Aligned presentations ---
//
// A comparison can be shown as a table, a scatter of the two published
// inputs, and a choropleth of one API-derived field. All three read the same
// rows; none of them may invent a value the response did not carry, and a
// geography missing a value on either side is excluded and counted rather
// than plotted at zero.

export interface ScatterPoint {
  geoId: string;
  name: string;
  /** Measure A's published value. */
  x: number;
  /** Measure B's published value. */
  y: number;
  /** The period each side's value describes, or `""` where none was published. */
  periodA: string;
  periodB: string;
  /** True when the two published periods differ, so the pair is not contemporaneous. */
  periodsDiffer: boolean;
}

export interface ScatterModel {
  points: ScatterPoint[];
  /** Geographies left out because one side published no usable number. */
  excluded: number;
  /**
   * Plotted points whose two sides describe different published periods.
   *
   * The route combines each side's own newest value rather than aligning
   * them to a shared period, and carries both periods so that is visible.
   * Counting it here lets the chart say so; the table already marks the row
   * (WEB-049).
   */
  differingPeriods: number;
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

const EMPTY_SCATTER: ScatterModel = Object.freeze({
  points: [],
  excluded: 0,
  differingPeriods: 0,
  minX: 0,
  maxX: 0,
  minY: 0,
  maxY: 0,
}) as ScatterModel;

/**
 * The two published inputs plotted against each other, one point per
 * geography.
 *
 * A scatter of the inputs is the honest aligned chart for a two-measure
 * comparison: it needs no shared axis or unit, and it shows each geography's
 * own pair rather than a series that would imply the two measures share a
 * scale. A geography whose either side published no usable number cannot be
 * a point — plotting it at zero would state a value neither source
 * published — so it is excluded and counted.
 */
export function comparisonScatterModel(
  response: ComparisonResponse | null | undefined,
): ScatterModel {
  const items = Array.isArray(response?.items) ? response.items : [];
  if (items.length === 0) {
    return EMPTY_SCATTER;
  }

  const points: ScatterPoint[] = [];
  let excluded = 0;
  for (const row of items) {
    const x = Number(row.value_a);
    const y = Number(row.value_b);
    if (
      row.value_a === null ||
      row.value_a === undefined ||
      row.value_b === null ||
      row.value_b === undefined ||
      !Number.isFinite(x) ||
      !Number.isFinite(y)
    ) {
      excluded += 1;
      continue;
    }
    points.push({
      geoId: String(row.geo_id ?? ""),
      name: comparisonRowName(row),
      x,
      y,
      periodA: String(row.period_a ?? ""),
      periodB: String(row.period_b ?? ""),
      periodsDiffer: periodsDiffer(row),
    });
  }

  if (points.length === 0) {
    return { ...EMPTY_SCATTER, excluded };
  }

  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  return {
    points,
    excluded,
    differingPeriods: points.filter((point) => point.periodsDiffer).length,
    minX: Math.min(...xs),
    maxX: Math.max(...xs),
    minY: Math.min(...ys),
    maxY: Math.max(...ys),
  };
}

/**
 * What the comparison map must say about the periods it coloured, or `""`.
 *
 * The map is the sharper of the two aligned views: it colours one number per
 * polygon, and that number is an API-derived subtraction or ratio between two
 * publications that may be years apart. "Coloured by difference" reads as a
 * difference at a time, and the route deliberately does not align its sides
 * to one.
 *
 * Empty when nothing differs, so this is a fact about the answer rather than
 * a standing disclaimer, and empty when the map is not drawn at all
 * (WEB-049).
 */
export function mapPeriodMismatchNote(
  response: ComparisonResponse | null | undefined,
  field: string,
): string {
  const rows = comparisonMapRows(response, field);
  if (rows.length === 0) {
    return "";
  }
  const items = Array.isArray(response?.items) ? response.items : [];
  const coloured = rows.filter((row) => row.value !== null).length;
  const differing = items.filter(
    (row, index) => rows[index]?.value !== null && periodsDiffer(row),
  ).length;
  if (differing === 0) {
    return "";
  }
  return (
    `${differing} of ${coloured} coloured geographies combine values published ` +
    "for different periods; each row's two periods are in the table below."
  );
}


function publishedCount(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

/**
 * What this comparison's geography count is an intersection of, or `""`.
 *
 * The route joins its two reduced sides on geography identity with an inner
 * join, so a geography only one side publishes is absent from the answer
 * entirely. "500 aligned geographies" then reads as the universe when it is
 * 500 of 3,143. API-087 serves each side's own count; this says it.
 *
 * Empty when both sides published exactly what was paired, so this is a fact
 * about the answer rather than a standing disclaimer, and empty when the API
 * publishes no counts at all -- an older deployment states no shortfall, and
 * reading an absent count as zero would report every geography as dropped
 * (WEB-050).
 */
export function describeComparisonCoverage(
  response: ComparisonResponse | null | undefined,
): string {
  const total = publishedCount(response?.total);
  const countA = publishedCount(response?.geographies_a);
  const countB = publishedCount(response?.geographies_b);
  if (total === null || countA === null || countB === null) {
    return "";
  }
  if (countA <= total && countB <= total) {
    return "";
  }
  const codeA = response?.metric_code_a || "measure A";
  const codeB = response?.metric_code_b || "measure B";
  return (
    `${formatNumber(total)} geographies are paired here. ` +
    `${codeA} publishes ${formatNumber(countA)} and ${codeB} publishes ` +
    `${formatNumber(countB)} under these filters; a geography only one of ` +
    "the two publishes is not in this comparison."
  );
}


/** The derived field a comparison map colours: the first the API named. */
export function defaultDerivedField(
  response: ComparisonResponse | null | undefined,
): string {
  const derivations = Array.isArray(response?.derivations) ? response.derivations : [];
  return derivations[0] || "";
}

/**
 * Comparison rows projected onto the observation row shape the shared
 * choropleth model reads, carrying one API-derived field as the value.
 *
 * The projection is deliberately thin: the geography attribution the map
 * joins on and the single derived value, as a string, exactly as the shared
 * model expects. A row whose derived field is null carries a null value and
 * is left uncoloured by that model rather than coloured as zero.
 */
export function comparisonMapRows(
  response: ComparisonResponse | null | undefined,
  field: string,
): ObservationRow[] {
  if (!field || !isDerivedField(response, field)) {
    return [];
  }
  const items = Array.isArray(response?.items) ? response.items : [];
  return items.map((row) => {
    const value = row[field];
    const usable = value !== null && value !== undefined && Number.isFinite(Number(value));
    return {
      geo_id: row.geo_id,
      geo_level: row.geo_level,
      state_fips: row.state_fips,
      county_fips: row.county_fips,
      state_name: row.state_name,
      county_name: row.county_name,
      value: usable ? String(value) : null,
      // Read by the shared choropleth model as the reason this geography
      // carries no number, and rendered into its legend after "Value not
      // published:" (WEB-078) -- so the words are the phrase that completes
      // that sentence rather than a sentence of their own.
      value_status: usable ? null : uncolouredReason(row, field),
    } as ObservationRow;
  });
}

/**
 * Why one geography in the answer carries no derived number.
 *
 * A single phrase covered every case and named the wrong one for the case
 * that actually happens. `/comparison` joins the two sides on geography, so
 * every row in the answer *is* on both sides -- "not on both sides" is the
 * reason a geography is missing from the answer, which is what
 * `geographies_a` / `geographies_b` report, not the reason a row inside it
 * has no ratio. The route computes no ratio where the denominator is zero,
 * and a zero is an ordinary published value for a count in a small county,
 * so that is the reason a reader actually meets (WEB-079).
 *
 * The sides' own missing values are kept as a case even though the four
 * sources the aligned routes accept all serve published numbers only
 * (API-127): a source that publishes a value state becoming analysis-ready
 * would make it reachable, and the phrase would otherwise be wrong again.
 */
function uncolouredReason(row: ComparisonRow, field: string): string {
  const published = (side: unknown): boolean =>
    side !== null && side !== undefined && Number.isFinite(Number(side));
  if (!published(row.value_a) || !published(row.value_b)) {
    return "one side published no number";
  }
  if (field === "ratio" && Number(row.value_b) === 0) {
    return "the denominator is zero";
  }
  return "not derived for this geography";
}

// ---------------------------------------------------------------------------
// Which grains a pair can be compared at (WEB-074)
// ---------------------------------------------------------------------------

/** One grain the set cannot be read at, and which measures removed it. */
export interface AbsentGrain {
  level: string;
  /** The metric codes that do not publish this grain. */
  withoutIt: string[];
}

/** The grains a set of measures can all be read at, and what narrowed it. */
export interface SharedGrainOffer {
  /** The grains every named measure publishes, in the vocabulary's order. */
  levels: string[];
  /** True when the offered set is narrower than the whole vocabulary. */
  narrowed: boolean;
  /** Why the list is narrow, in the publisher's terms. "" when it is not. */
  note: string;
  /** A grain that was asked for and the set does not publish. "" otherwise. */
  unavailable: string;
  /** Per unoffered grain, the measures that do not publish it. */
  absent: AbsentGrain[];
}

/** The grains a comparison offers, and what it could not offer. */
export interface ComparisonGrainOffer {
  /** The grains both sides publish, in the published vocabulary's order. */
  levels: string[];
  /** True when the offered set is narrower than the whole vocabulary. */
  narrowed: boolean;
  /** Why the list is narrow, in the publisher's terms. "" when it is not. */
  note: string;
  /** A grain that was asked for and neither side publishes. "" otherwise. */
  unavailable: string;
}

function publishedGrains(metric: MetricSummary | null | undefined): string[] {
  const declared = metricSupportedGeoLevels(metric);
  // A measure that declares no grains is a measure whose grains are unknown,
  // which is not the same as one published at none — the explorer's own rule
  // (WEB-038), so the whole vocabulary stays offered for it.
  return declared.length > 0 ? declared : [...GEO_GRAIN_ORDER];
}

function grainWords(levels: readonly string[]): string {
  return levels.map((level) => GEO_GRAIN_LABELS[level]?.one || level).join(", ");
}

/**
 * The grains a set of measures can all be read at, and what narrowed it.
 *
 * The offer is the *intersection*: a cross-sectional reading is answered at
 * one grain, so a grain only some of the measures publish is a grain the set
 * cannot be read at. A measure declaring no grains does not narrow the offer,
 * because unknown is not none — the explorer's own rule (WEB-038).
 *
 * Written for any number of measures because the comparison workspace asks it
 * of two and the workbench asks it of two to eight. One implementation rather
 * than two, so the two screens cannot come to different conclusions about the
 * same publication.
 *
 * `absent` names, per grain the whole vocabulary offers but this set does not,
 * which measures failed to publish it — criterion 1's "says which publisher
 * removed each absent grain". Without it a reader sees a shorter list of
 * grains with no way to tell which of their measures shortened it.
 */
export function sharedGrainOffer({
  metrics,
  requested,
}: {
  metrics: readonly (MetricSummary | null | undefined)[];
  requested?: string | null;
}): SharedGrainOffer {
  const named = metrics.filter(
    (metric): metric is MetricSummary => Boolean(metric),
  );
  const declared = named.map((metric) => ({
    metric,
    grains: publishedGrains(metric),
  }));

  const levels = GEO_GRAIN_ORDER.filter((level) =>
    declared.every((entry) => entry.grains.includes(level)),
  );
  const narrowed = levels.length < GEO_GRAIN_ORDER.length;

  const absent = GEO_GRAIN_ORDER.filter(
    (level) => !levels.includes(level),
  ).map((level) => ({
    level,
    // Only the measures that actually fail to publish it. A measure declaring
    // nothing is not among them: it did not remove the grain, and naming it
    // would report an absence the publication does not claim.
    withoutIt: declared
      .filter((entry) => !entry.grains.includes(level))
      .map((entry) => String(entry.metric.metric_code)),
  }));

  let note = "";
  if (named.length > 0 && narrowed) {
    note =
      levels.length === 0
        ? "These measures publish no geography grain in common, so there is " +
          "no level to read them at together. A cross-sectional answer is " +
          "read at one grain; this is the publishers' declaration, not a " +
          "limit of this screen."
        : `These measures are all published at ${grainWords(levels)}, so the ` +
          "other view levels are not offered. A cross-sectional answer is " +
          "read at one grain, so a grain only some of them publish cannot " +
          "be read here.";
  }

  const wanted = normalizeGeoLevel(requested);
  const unavailable =
    wanted && !levels.includes(wanted)
      ? `This link asked to read at ${
          GEO_GRAIN_LABELS[wanted]?.one || wanted
        }, which these measures do not all publish` +
        (levels.length > 0 ? `; showing ${grainWords(levels.slice(0, 1))}.` : ".")
      : "";

  return { levels: [...levels], narrowed, note, unavailable, absent };
}

/**
 * The grains a pair of measures can be compared at.
 *
 * The workspace hard-coded `NATIONAL`, `STATE`, `COUNTY` and ignored what
 * either side publishes, while `parseComparisonState` accepts all five words
 * and the workspace assigned the parsed value straight into the selection. So
 * a `?geo_level=PLACE` link — reachable data: the analysis routes serve
 * Census PEP, which publishes places — put a value in the select that no
 * option carried, and the control showed one grain while the request sent
 * another (WEB-074).
 *
 * The pair's own wording is kept ("these two measures", "compare") because it
 * is what the comparison workspace says, and a screen about a pair should not
 * start talking about a set. The decision underneath is `sharedGrainOffer`'s.
 */
export function comparisonGrainOffer({
  metricA,
  metricB,
  requested,
}: {
  metricA: MetricSummary | null | undefined;
  metricB: MetricSummary | null | undefined;
  requested?: string | null;
}): ComparisonGrainOffer {
  const shared = sharedGrainOffer({ metrics: [metricA, metricB], requested });
  const named = Boolean(metricA || metricB);

  let note = "";
  if (named && shared.narrowed) {
    note =
      shared.levels.length === 0
        ? "These two measures publish no geography grain in common, so there " +
          "is no level to compare them at. A comparison is answered at one " +
          "grain; this is the publishers' declaration, not a limit of this " +
          "screen."
        : `These measures are both published at ${grainWords(shared.levels)}, so the ` +
          "other view levels are not offered for the pair. A comparison is " +
          "answered at one grain, so a grain only one side publishes cannot " +
          "be read here.";
  }

  const wanted = normalizeGeoLevel(requested);
  const unavailable =
    wanted && !shared.levels.includes(wanted)
      ? `This link asked to compare at ${
          GEO_GRAIN_LABELS[wanted]?.one || wanted
        }, which the pair does not both publish` +
        (shared.levels.length > 0
          ? `; showing ${grainWords(shared.levels.slice(0, 1))}.`
          : ".")
      : "";

  return { levels: [...shared.levels], narrowed: shared.narrowed, note, unavailable };
}

/** The grain to show when the asked-for one is not offered. */
export function preferredComparisonGrain(
  levels: readonly string[],
  fallback = "COUNTY",
): string {
  if (levels.length === 0) {
    return "";
  }
  return levels.includes(fallback) ? fallback : (levels[0] ?? "");
}
