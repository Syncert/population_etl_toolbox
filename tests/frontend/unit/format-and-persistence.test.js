import { beforeEach, describe, expect, test } from "vitest";

// Covers: WEB-001 — formatting and saved-chart persistence are deterministic.

import { displayMetricName } from "../../../apps/web/lib/format";
import {
  SAVED_CHARTS_KEY,
  readSavedCharts,
  saveChart,
} from "../../../apps/web/lib/savedCharts";

describe("frontend formatting and saved-chart persistence", () => {
  beforeEach(() => window.localStorage.clear());

  test("formats Census metric labels and safe empty values", () => {
    expect(displayMetricName({ metric_code: "ACS:acs5:B01003_001" })).toBe("Total population");
    expect(displayMetricName({ metric_display_name: "Estimate!!Population!Total" })).toBe("Population - Total");
    expect(displayMetricName(null)).toBe("Untitled metric");
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
