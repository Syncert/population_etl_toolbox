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
import type { ObservationRow } from "./explorerViewModel";

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
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: 4 });
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
 * The export carries its own interpretation envelope: both measures and
 * their sources and units, each row's own published values and periods, the
 * derived fields marked as derived in the heading itself, and every caveat
 * the verdict published — so the file can be read outside this application
 * without losing what the API said about it.
 */
export function comparisonExport(
  response: ComparisonResponse | null | undefined,
  preflight: ComparisonPreflight | null | undefined,
): ComparisonExport {
  const items = Array.isArray(response?.items) ? response.items : [];
  const derivations = Array.isArray(response?.derivations) ? response.derivations : [];
  const codeA = response?.metric_code_a || "";
  const codeB = response?.metric_code_b || "";
  const caveats = [
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
  return {
    headings,
    rows,
    filename: `comparison-${slug(codeA)}-vs-${slug(codeB)}.csv`,
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
}

export interface ScatterModel {
  points: ScatterPoint[];
  /** Geographies left out because one side published no usable number. */
  excluded: number;
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
}

const EMPTY_SCATTER: ScatterModel = Object.freeze({
  points: [],
  excluded: 0,
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
    points.push({ geoId: String(row.geo_id ?? ""), name: comparisonRowName(row), x, y });
  }

  if (points.length === 0) {
    return { ...EMPTY_SCATTER, excluded };
  }

  const xs = points.map((point) => point.x);
  const ys = points.map((point) => point.y);
  return {
    points,
    excluded,
    minX: Math.min(...xs),
    maxX: Math.max(...xs),
    minY: Math.min(...ys),
    maxY: Math.max(...ys),
  };
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
      value_status: usable ? null : "not published on both sides",
    } as ObservationRow;
  });
}
