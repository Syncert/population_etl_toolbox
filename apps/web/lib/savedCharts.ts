export const SAVED_CHARTS_KEY = "economic-data-studio:saved-charts:v1";
export const BUILDER_DRAFT_KEY = "economic-data-studio:builder-draft:v1";

/**
 * How many saved views this browser keeps.
 *
 * The cap is not new; what is new is that reaching it is *reported*. The
 * write used to `.slice(0, 50)` and say nothing, so a reader saving their
 * fifty-first view lost their first one with no sign on screen that anything
 * had been removed.
 */
export const SAVED_CHART_LIMIT = 50;

export function readSavedCharts(): SavedChart[] {
  if (typeof window === "undefined") {
    return [];
  }
  try {
    const value: unknown = JSON.parse(
      window.localStorage.getItem(SAVED_CHARTS_KEY) || "[]",
    );
    // Anything in this store was written by an earlier version of this
    // application, so it is read as data rather than trusted as a shape: an
    // entry without an id cannot be replaced or removed and is dropped.
    return Array.isArray(value)
      ? value.filter(
          (entry): entry is SavedChart =>
            Boolean(entry) && typeof (entry as SavedChart).id === "string",
        )
      : [];
  } catch {
    return [];
  }
}

/** A saved view: an addressable id, and whatever the screen chose to keep. */
export interface SavedChart {
  id: string;
  [key: string]: unknown;
}

/**
 * Why this browser would not keep something, in words a reader can act on.
 *
 * Browser storage does not fail by returning null. It throws: a
 * `QuotaExceededError` when the origin's allowance is spent, and a
 * `SecurityError` — or an exception from the `localStorage` accessor itself,
 * before any method is called — where the browser has site data turned off,
 * which is the ordinary state of a private window in several browsers. An
 * unwrapped `setItem` in a click handler therefore throws out of the handler,
 * and there was nowhere for it to land.
 *
 */
export function storageRefusalReason(error: unknown): string {
  const name =
    (error && typeof error === "object" && "name" in error
      ? String((error as { name?: unknown }).name)
      : "") || "";
  if (name === "QuotaExceededError" || name === "NS_ERROR_DOM_QUOTA_REACHED") {
    return "this browser has no room left for saved views";
  }
  if (name === "SecurityError") {
    return "this browser has site storage turned off";
  }
  return "this browser refused to store it";
}

export interface LocalWriteResult {
  outcome: "saved" | "refused";
  /** How many entries the cap dropped. Always 0 on a refusal. */
  evicted: number;
  /** Why the browser refused. Empty on a save. */
  reason: string;
}

/**
 * Write a value to browser storage, reporting what happened.
 *
 * Never throws: a refused save is a thing to say on the control that caused
 * it, not an exception in a click handler, and the analysis on screen has to
 * survive it.
 *
 */
function write(key: string, value: unknown, evicted = 0): LocalWriteResult {
  if (typeof window === "undefined") {
    return { outcome: "refused", evicted: 0, reason: "there is no browser to save in" };
  }
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    return { outcome: "refused", evicted: 0, reason: storageRefusalReason(error) };
  }
  return { outcome: "saved", evicted, reason: "" };
}

/**
 * Save a view in this browser, reporting the outcome.
 *
 */
export function saveChart(
  chart: SavedChart,
): LocalWriteResult & { charts: SavedChart[] } {
  const charts = readSavedCharts();
  const merged = [chart, ...charts.filter((item) => item.id !== chart.id)];
  const next = merged.slice(0, SAVED_CHART_LIMIT);
  const result = write(SAVED_CHARTS_KEY, next, merged.length - next.length);
  // A refused write leaves the store as it was, so the caller is told what is
  // actually there rather than what it tried to put there.
  return { ...result, charts: result.outcome === "saved" ? next : charts };
}

/**
 * Save the evidence-packet draft in this browser, reporting the outcome.
 *
 */
export function saveBuilderDraft(packet: unknown): LocalWriteResult {
  return write(BUILDER_DRAFT_KEY, packet);
}
