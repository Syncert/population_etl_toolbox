import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join, relative } from "node:path";

import { beforeEach, describe, expect, test } from "vitest";

// Covers: WEB-001 — formatting and saved-chart persistence are deterministic.
// Covers: WEB-105 — every number, date and time is formatted in one locale,
// chosen once in `lib/format`, and no module reaches past it to the viewer's
// own locale.

import {
  DISPLAY_LOCALE,
  displayMetricName,
  formatDate,
  formatNumber,
  formatTime,
} from "../../../apps/web/lib/format";
import {
  SAVED_CHARTS_KEY,
  readSavedCharts,
  saveChart,
} from "../../../apps/web/lib/savedCharts";

describe("frontend formatting and saved-chart persistence", () => {
  beforeEach(() => window.localStorage.clear());

  test("formats Census metric labels and safe empty values", () => {
    expect(displayMetricName({ metric_code: "CENSUS_ACS:acs5:B01003_001" })).toBe("Total population");
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
