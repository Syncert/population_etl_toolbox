/**
 * The one locale this application formats numbers, dates and times in.
 *
 * It is a constant, not a preference read from the browser. Twenty-odd call
 * sites used to call `toLocaleString()` with no locale, which resolves to
 * whatever the *viewer's* browser is set to, while `explorerViewModel` asked
 * for `en-US` explicitly. In a browser set to de-DE that put `1.234,5` in the
 * legend and `1,234.5` in the chart beside it -- two renderings of one number
 * on one screen.
 *
 * Choosing one locale is not localisation and is not a claim that `en-US` is
 * the right locale for every reader. It is a claim that the application must
 * be internally consistent, which it was not. Localising is a separate
 * decision; when it is taken, it is taken here, once.
 */
export const DISPLAY_LOCALE = "en-US";

/**
 * Format a number for display.
 *
 * A value that is not a finite number returns an empty string rather than
 * `"NaN"`: this module renders, and it has nothing to say about a value that
 * is not one. A caller that must distinguish "not published" from "nothing to
 * show" says so itself -- `comparisonValueText` does -- because only the
 * caller knows which of the two it is holding.
 *
 * @param {unknown} value
 * @param {Intl.NumberFormatOptions} [options]
 * @returns {string}
 */
export function formatNumber(value, options) {
  // `Number(null)` is 0 and `Number("")` is 0, so the coercion cannot be the
  // only guard: the one thing this module must never do is turn an absence
  // into a zero.
  if (value === null || value === undefined || value === "") return "";
  const numeric = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numeric)) return "";
  return numeric.toLocaleString(DISPLAY_LOCALE, options);
}

/**
 * Format a date for display. Accepts a `Date` or anything `new Date` parses.
 *
 * An unparseable value returns an empty string, for the same reason
 * `formatNumber` does.
 *
 * @param {Date | string | number | null | undefined} value
 * @param {Intl.DateTimeFormatOptions} [options]
 * @returns {string}
 */
export function formatDate(value, options) {
  const date = value instanceof Date ? value : new Date(/** @type {any} */ (value));
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString(DISPLAY_LOCALE, options);
}

/**
 * Format a time of day for display.
 *
 * @param {Date | string | number | null | undefined} value
 * @param {Intl.DateTimeFormatOptions} [options]
 * @returns {string}
 */
export function formatTime(value, options) {
  const date = value instanceof Date ? value : new Date(/** @type {any} */ (value));
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString(DISPLAY_LOCALE, options);
}

export function displayMetricName(metric) {
  if (typeof metric === "object" && metric?.metric_code?.endsWith("B01003_001")) {
    return "Total population";
  }
  const value = typeof metric === "string" ? metric : metric?.metric_display_name;
  if (!value) return "Untitled metric";
  return value
    .replaceAll("!!", " - ")
    .replaceAll("!", " - ")
    .replace(/^Estimate\s*-\s*/i, "")
    .replace(/\s+/g, " ")
    .trim();
}
