// The map-display oracle: what a latest answer's rows say an explorer map
// should show, computed from the rows alone (WEB-118).
//
// Shared by the data-path sweep (`smoke/map-display.smoke.test.js`) and the
// painted-pixel check (`live/map-paint.live.spec.js`), and imported by
// neither the application nor its tests: it must stay an independent reading
// of the rows, or it would agree with the page's mistakes.

function isNumber(value) {
  if (value === null || value === undefined || value === "") {
    return false;
  }
  return Number.isFinite(Number(value));
}

function periodOf(row) {
  return `${row.period_start ?? ""}|${row.period_end ?? ""}`;
}

/**
 * What the rows say the map should show, computed without the application.
 *
 * Deliberately plain: group by `geo_id`, look for two rows sharing one
 * geography and one period, and take each geography's newest period by its
 * published bounds.
 */
export function oracle(rows) {
  const byGeography = new Map();
  for (const row of rows) {
    const geoId = row.geo_id ? String(row.geo_id) : "";
    if (!geoId) {
      continue;
    }
    const list = byGeography.get(geoId) || [];
    list.push(row);
    byGeography.set(geoId, list);
  }
  let stratified = false;
  let colourable = 0;
  for (const list of byGeography.values()) {
    const periods = new Set();
    for (const row of list) {
      const key = periodOf(row);
      if (periods.has(key)) {
        stratified = true;
      }
      periods.add(key);
    }
    const newest = list.reduce((best, row) => (periodOf(row) > periodOf(best) ? row : best));
    if (isNumber(newest.value)) {
      colourable += 1;
    }
  }
  const numeric = rows.some((row) => isNumber(row.value));
  return { rows: rows.length, geographies: byGeography.size, stratified, colourable, numeric };
}

/** Grade one map: the application's answer against the oracle's. */
export function grade(expected, view, model) {
  if (expected.rows === 0) {
    // The catalog offered this map; the read answered nothing at all. That is
    // a reader choosing a map the warehouse cannot fill -- the catalog
    // advertising a grain its publisher no longer serves -- not an honest
    // "every value withheld".
    return {
      verdict: "fail",
      problem: "the catalog advertises this grain and the read answered no rows",
    };
  }
  if (!expected.numeric) {
    return { verdict: "empty", problem: null };
  }
  if (expected.stratified) {
    if (!view.stratification.stratified) {
      return {
        verdict: "fail",
        problem: "several series share a geography and period, and the map would keep whichever arrived last",
      };
    }
    if (view.stratification.varyingDimensions.length === 0) {
      return { verdict: "fail", problem: "declined as stratified without naming what varies" };
    }
    return { verdict: "declined", problem: null };
  }
  if (view.stratification.stratified) {
    return {
      verdict: "fail",
      problem: `one series per geography, declined as stratified by ${view.stratification.varyingDimensions.join(", ") || "nothing named"}`,
    };
  }
  if (model.valueCount !== expected.colourable) {
    return {
      verdict: "fail",
      problem: `${expected.colourable} geographies carry a newest value; the map colours ${model.valueCount}`,
    };
  }
  return { verdict: "coloured", problem: null };
}
