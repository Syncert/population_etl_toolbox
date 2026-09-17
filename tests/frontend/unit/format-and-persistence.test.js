import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";

import { afterEach, beforeEach, describe, expect, test } from "vitest";

// Covers: WEB-001 — formatting and saved-chart persistence are deterministic.
// Covers: WEB-105 — every number, date and time is formatted in one locale,
// chosen once in `lib/format`, and no module reaches past it to the viewer's
// own locale.
// Covers: WEB-106 — a write to browser storage cannot throw into a click
// handler: it returns an outcome, a refusal is reported in words, an
// eviction at the cap is reported rather than silent, and what is already
// stored survives a refused write.

import {
  DISPLAY_LOCALE,
  displayMetricName,
  formatDate,
  formatNumber,
  formatTime,
} from "../../../apps/web/lib/format";
import { describeLocalSave } from "../../../apps/web/lib/savedAnalysis";
import {
  BUILDER_DRAFT_KEY,
  SAVED_CHARTS_KEY,
  SAVED_CHART_LIMIT,
  readSavedCharts,
  saveBuilderDraft,
  saveChart,
} from "../../../apps/web/lib/savedCharts";

describe("frontend formatting and saved-chart persistence", () => {
  beforeEach(() => window.localStorage.clear());

  test("a metric's name is the catalog's, never this client's", () => {
    // Covers: WEB-112 — this returned "Total population" for any metric code
    // ending in one Census variable, whatever `metric_display_name` the
    // catalog answered. A name only this client can change is a name
    // republishing the catalog cannot fix.
    expect(
      displayMetricName({
        metric_code: "CENSUS_ACS:acs5:B01003_001",
        metric_display_name: "Estimate!!Total:",
      }),
    ).toBe("Total:");
    // With no published name there is nothing to show but the absence.
    expect(displayMetricName({ metric_code: "CENSUS_ACS:acs5:B01003_001" })).toBe(
      "Untitled metric",
    );
    expect(displayMetricName({ metric_display_name: "Estimate!!Population!Total" })).toBe("Population - Total");
    expect(displayMetricName(null)).toBe("Untitled metric");
  });

  test("formats numbers, dates and times in the one chosen locale", () => {
    expect(DISPLAY_LOCALE).toBe("en-US");
    expect(formatNumber(1234567)).toBe("1,234,567");
    expect(formatNumber(1234.5678, { maximumFractionDigits: 3 })).toBe("1,234.568");
    expect(formatDate("2026-09-16T00:00:00Z")).toBe("9/16/2026");
    expect(
      formatTime("2026-09-16T13:05:00Z", { hour: "numeric", minute: "2-digit" }),
    ).toBe("1:05 PM");
  });

  test("renders nothing, rather than NaN, for a value that is not a number", () => {
    // The module renders. It has no standing to say *why* a value is missing,
    // and "NaN" on a screen is a defect report addressed to the wrong reader.
    expect(formatNumber(undefined)).toBe("");
    expect(formatNumber(null)).toBe("");
    expect(formatNumber("not a number")).toBe("");
    expect(formatDate("not a date")).toBe("");
    expect(formatTime(undefined)).toBe("");
  });

  test("no module formats past `lib/format` to the viewer's own locale", () => {
    // `toLocaleString()` with no locale resolves to whatever the *viewer's*
    // browser is set to. Twenty-odd call sites did that while
    // `explorerViewModel` asked for `en-US`, so a browser set to de-DE put
    // `1.234,5` in the legend and `1,234.5` in the chart beside it. The rule
    // is mechanical, so it is checked mechanically rather than by review.
    //
    // A literal locale handed to `Intl` is the same defect spelled the other
    // way round -- `explorerViewModel` asked for `en-US` by name -- so the
    // rule covers both: the locale is `DISPLAY_LOCALE`, or it is not chosen
    // here.
    // Walked up to, not resolved from `import.meta.url`: this tier runs in
    // jsdom, where that is an http URL served by vite.
    let root = process.cwd();
    for (;;) {
      try {
        statSync(join(root, "apps", "web", "package.json"));
        break;
      } catch {
        const parent = dirname(root);
        if (parent === root) throw new Error(`apps/web not found from ${process.cwd()}`);
        root = parent;
      }
    }
    const web = join(root, "apps", "web");
    const allowed = join(web, "lib", "format.js");
    const bare = /\.toLocale(String|DateString|TimeString)\(|new Intl\.(Number|DateTime)Format\(\s*["']/;

    const offences = [];
    const walk = (directory) => {
      for (const entry of readdirSync(directory)) {
        if (entry === "node_modules" || entry.startsWith(".")) continue;
        const path = join(directory, entry);
        if (statSync(path).isDirectory()) {
          walk(path);
          continue;
        }
        if (!/\.(js|jsx|ts|tsx)$/.test(entry) || path === allowed) continue;
        const source = readFileSync(path, "utf8");
        source.split("\n").forEach((line, index) => {
          if (bare.test(line)) {
            offences.push(`${relative(web, path)}:${index + 1}: ${line.trim()}`);
          }
        });
      }
    };
    for (const folder of ["app", "components", "lib"]) walk(join(web, folder));

    expect(
      offences,
      "format through lib/format: formatNumber, formatDate, formatTime",
    ).toEqual([]);
  });

  test("persists, replaces, caps, and recovers saved charts", () => {
    for (let index = 0; index < 55; index += 1) {
      saveChart({ id: `chart-${index}`, title: `Chart ${index}` });
    }
    expect(readSavedCharts()).toHaveLength(50);
    expect(readSavedCharts()[0].id).toBe("chart-54");

    saveChart({ id: "chart-54", title: "Revised chart" });
    expect(readSavedCharts()).toHaveLength(50);
    expect(readSavedCharts()[0]).toEqual({ id: "chart-54", title: "Revised chart" });

    window.localStorage.setItem(SAVED_CHARTS_KEY, "not-json");
    expect(readSavedCharts()).toEqual([]);
  });
});


describe("a refused browser save is reported, never thrown", () => {
  const realStorage = window.localStorage;

  function withStorage(replacement) {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      get: replacement,
    });
  }

  afterEach(() => {
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      get: () => realStorage,
    });
    realStorage.clear();
  });

  test("a full store refuses the save and says so, keeping what is stored", () => {
    realStorage.clear();
    saveChart({ id: "first", title: "The analysis on screen" });
    const before = readSavedCharts();
    expect(before).toHaveLength(1);

    // What a browser actually does when the origin's allowance is spent. It
    // throws; it does not return null, and an unwrapped `setItem` in a click
    // handler threw straight out of the handler.
    // Delegating explicitly rather than inheriting: `Storage` reads through
    // internal slots, so a prototype-based stand-in answers nothing.
    const full = {
      getItem: (key) => realStorage.getItem(key),
      removeItem: (key) => realStorage.removeItem(key),
      clear: () => realStorage.clear(),
      setItem: () => {
        const error = new Error("quota");
        error.name = "QuotaExceededError";
        throw error;
      },
    };
    withStorage(() => full);

    const result = saveChart({ id: "second", title: "Refused" });
    expect(result.outcome).toBe("refused");
    expect(result.evicted).toBe(0);
    expect(result.reason).toMatch(/no room left/);
    // The store is reported as it actually is, not as the write intended.
    expect(result.charts).toEqual(before);

    const outcome = describeLocalSave(result, "Refused", SAVED_CHART_LIMIT);
    expect(outcome.state).toBe("bad");
    expect(outcome.destination).toBeNull();
    expect(outcome.message).toMatch(/Not saved/);
    // The reader is told what is still true, which is the thing they care
    // about: the work they are looking at did not go anywhere.
    expect(outcome.message).toMatch(/analysis on screen is unchanged/);
  });

  test("a browser with storage turned off refuses from the accessor itself", () => {
    // In a private window several browsers throw from `window.localStorage`
    // before any method is reached, so the guard cannot be on `setItem` alone.
    withStorage(() => {
      const error = new Error("denied");
      error.name = "SecurityError";
      throw error;
    });

    const result = saveChart({ id: "third", title: "Refused" });
    expect(result.outcome).toBe("refused");
    expect(result.reason).toMatch(/site storage turned off/);
    expect(result.charts).toEqual([]);
    expect(saveBuilderDraft({ title: "draft" }).outcome).toBe("refused");
    expect(
      describeLocalSave(result, "Refused", SAVED_CHART_LIMIT).message,
    ).toMatch(/storage turned off/);
  });

  test("the fifty-first view is saved, and the eviction is reported", () => {
    realStorage.clear();
    for (let index = 0; index < SAVED_CHART_LIMIT; index += 1) {
      expect(saveChart({ id: `chart-${index}` }).evicted).toBe(0);
    }

    const result = saveChart({ id: "one-too-many" });
    expect(result.outcome).toBe("saved");
    expect(result.evicted).toBe(1);
    expect(result.charts).toHaveLength(SAVED_CHART_LIMIT);
    // The cap dropped the oldest, which it always did. What is new is that
    // the reader is told.
    expect(result.charts.some((chart) => chart.id === "chart-0")).toBe(false);

    const outcome = describeLocalSave(result, "one-too-many", SAVED_CHART_LIMIT);
    expect(outcome.state).toBe("warn");
    expect(outcome.destination).toBe("browser");
    expect(outcome.message).toMatch(/keeps 50 saved views/);
    expect(outcome.message).toMatch(/1 oldest view made way/);
  });

  test("the builder draft reports its own write", () => {
    realStorage.clear();
    expect(saveBuilderDraft({ title: "draft" })).toEqual({
      outcome: "saved",
      evicted: 0,
      reason: "",
    });
    expect(JSON.parse(realStorage.getItem(BUILDER_DRAFT_KEY)).title).toBe("draft");
  });
});
