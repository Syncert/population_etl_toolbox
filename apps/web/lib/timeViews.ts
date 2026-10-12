// How the explorer asks for a metric over calendar windows (ADR-0007).
//
// The API answers a metric at its native periods, at calendar quarters and
// years (`time_grain`), and over trailing or year-to-date windows (`window`).
// Which of those a metric has is published on `/catalog/metrics/{code}` as
// `time_grains` and `time_windows`, read from the same declarations the
// route serves by, so this module offers exactly those and never one the
// route would refuse. Every calendar or window row says where its figure
// came from in `derivation`: the provider's own figure, or one the warehouse
// derived with a reviewed method. A window missing a month carries the
// reason instead of a value, and the explorer shows the reason rather than
// "No observation" -- a gap is not an absence of data, it is a refusal.

export type TimeView =
  | "native"
  | "quarterly"
  | "annual"
  | "trailing_3"
  | "trailing_12"
  | "ytd";

export const NATIVE_TIME_VIEW: TimeView = "native";

const GRAIN_VIEWS: readonly TimeView[] = ["quarterly", "annual"];
const WINDOW_VIEWS: readonly TimeView[] = ["trailing_3", "trailing_12", "ytd"];

export const TIME_VIEW_LABELS: Readonly<Record<TimeView, string>> = {
  native: "As published",
  quarterly: "Calendar quarters",
  annual: "Calendar years",
  trailing_3: "Trailing 3 months",
  trailing_12: "Trailing 12 months",
  ytd: "Year to date",
};

export interface MetricTimeCapability {
  time_grains?: readonly string[] | null;
  time_windows?: readonly string[] | null;
}

/** The views this metric answers, in the order a reader meets them. */
export function offeredTimeViews(capability: MetricTimeCapability | null | undefined): TimeView[] {
  const grains = new Set(capability?.time_grains || []);
  const windows = new Set(capability?.time_windows || []);
  return [
    NATIVE_TIME_VIEW,
    ...GRAIN_VIEWS.filter((view) => grains.has(view)),
    ...WINDOW_VIEWS.filter((view) => windows.has(view)),
  ];
}

export function isWindowView(view: TimeView): boolean {
  return WINDOW_VIEWS.includes(view);
}

/** The parameters that ask `/observations` for this view; none for native. */
export function timeViewParams(view: TimeView | null | undefined): Record<string, string> {
  if (!view || view === NATIVE_TIME_VIEW) return {};
  return isWindowView(view) ? { window: view } : { time_grain: view };
}

interface Derivation {
  kind?: string | null;
  method?: string | null;
  method_version?: number | null;
  expected_periods?: number | null;
  present_periods?: number | null;
  refusal_reason?: string | null;
}

function derivationOf(row: unknown): Derivation | null {
  if (!row || typeof row !== "object") return null;
  const derivation = (row as { derivation?: unknown }).derivation;
  return derivation && typeof derivation === "object" ? (derivation as Derivation) : null;
}

/** Why a calendar or window row has no value, or null when it has one. */
export function refusalReason(row: unknown): string | null {
  const reason = derivationOf(row)?.refusal_reason;
  return typeof reason === "string" && reason ? reason : null;
}

/** The refusal shown where a value would be: "11 of 12 months reported". */
export function refusalLabel(row: unknown): string | null {
  const derivation = derivationOf(row);
  if (!refusalReason(row) || !derivation) return null;
  const present = derivation.present_periods;
  const expected = derivation.expected_periods;
  if (typeof present === "number" && typeof expected === "number") {
    return `Incomplete window: ${present} of ${expected} months reported`;
  }
  return "Incomplete window";
}

const METHOD_WORDS: Readonly<Record<string, string>> = {
  mean: "mean",
  sum: "sum",
};

/**
 * A few words for a legend entry and its hover: whose figures a time-view
 * series draws, with the method when the warehouse derived them. Null when
 * the rows carry no derivation (a native read).
 */
export function derivationLabel(rows: readonly unknown[] | null | undefined): string | null {
  const derivations = (rows || []).map(derivationOf).filter(Boolean) as Derivation[];
  if (derivations.length === 0) return null;
  const provider = derivations.some((d) => d.kind === "provider_published");
  const derived = derivations.find((d) => d.kind === "derived");
  const method = derived
    ? `derived (${METHOD_WORDS[String(derived.method)] || String(derived.method || "reviewed method")})`
    : null;
  if (provider && method) return `provider-published and ${method}`;
  return provider ? "provider-published" : method;
}

/**
 * One caption for a set of rows: what kind of figure the reader is looking
 * at. Provider-published and derived rows can share a page (BLS's own annual
 * average beside a derived year it did not publish), and the caption says so
 * rather than naming whichever row came first.
 */
export function derivationCaption(rows: readonly unknown[] | null | undefined): string | null {
  const derivations = (rows || []).map(derivationOf).filter(Boolean) as Derivation[];
  if (derivations.length === 0) return null;
  const provider = derivations.some((d) => d.kind === "provider_published");
  const derived = derivations.filter((d) => d.kind === "derived");
  const parts: string[] = [];
  if (provider) parts.push("Provider-published figures");
  if (derived.length > 0) {
    const first = derived[0] as Derivation;
    const method = METHOD_WORDS[String(first.method)] || String(first.method || "reviewed method");
    const months = derived.find((d) => typeof d.expected_periods === "number")?.expected_periods;
    const sameSpan = derived.every((d) => d.expected_periods === months);
    const span =
      typeof months === "number" && sameSpan ? ` of ${months} monthly values` : " of monthly values";
    parts.push(
      `Derived by the warehouse: ${method}${span}${
        typeof first.method_version === "number" ? ` (method v${first.method_version})` : ""
      }`,
    );
  }
  const refused = derived.filter((d) => d.refusal_reason).length;
  if (refused > 0) {
    parts.push(`${refused} incomplete window${refused === 1 ? "" : "s"} shown without a value`);
  }
  return parts.join(". ") + ".";
}

function hasValue(row: { value?: unknown }): boolean {
  return row.value !== null && row.value !== undefined && row.value !== "";
}

/**
 * One calendar window per geography for the map: its newest complete window,
 * or -- where it has none -- its newest window, which then paints as its
 * reason. The newest calendar year is usually still under way, so mapping
 * the newest window alone would refuse every polygon; this keeps the most
 * recent figure the provider's months support and still shows a geography
 * that has none as refused rather than absent.
 */
export function mapWindowRows<T extends { geo_id?: unknown; value?: unknown; period_start?: unknown }>(
  rows: readonly T[] | null | undefined,
): T[] {
  const chosen = new Map<string, T>();
  for (const row of rows || []) {
    const key = String(row.geo_id ?? "");
    const current = chosen.get(key);
    if (!current) {
      chosen.set(key, row);
      continue;
    }
    const better =
      hasValue(row) !== hasValue(current)
        ? hasValue(row)
        : String(row.period_start ?? "") > String(current.period_start ?? "");
    if (better) chosen.set(key, row);
  }
  return [...chosen.values()];
}

const TIME_VIEWS: readonly TimeView[] = [NATIVE_TIME_VIEW, ...GRAIN_VIEWS, ...WINDOW_VIEWS];

/** A view word from a link or a stored document, or native for anything else. */
export function parseTimeView(value: unknown): TimeView {
  return TIME_VIEWS.includes(value as TimeView) ? (value as TimeView) : NATIVE_TIME_VIEW;
}

/** The view a stored observations read names in `time_grain` and `window`. */
export function timeViewFromDocument(read: {
  time_grain?: string | null;
  window?: string | null;
}): TimeView {
  if (read.window) return parseTimeView(read.window);
  return parseTimeView(read.time_grain || NATIVE_TIME_VIEW);
}

/** The `time_grain` and `window` a stored read records for a view. */
export function timeViewDocumentFields(view: TimeView | null | undefined): {
  time_grain: "native" | "quarterly" | "annual";
  window: "trailing_3" | "trailing_12" | "ytd" | null;
} {
  if (!view || view === NATIVE_TIME_VIEW) return { time_grain: "native", window: null };
  if (isWindowView(view)) {
    return { time_grain: "native", window: view as "trailing_3" | "trailing_12" | "ytd" };
  }
  return { time_grain: view as "quarterly" | "annual", window: null };
}

