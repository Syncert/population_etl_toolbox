// Covers: WEB-158 — the explorer's status row says how many years of a
// measure are published, from the period listing, in words a reader can
// check. It counted release identities, which for Census SAIPE are a load's
// capture time: Dane County median household income read "11 published
// releases" where the warehouse held 2023 and 2024 (OFFICE_LAB, 2026-11-03).
import { describe, expect, it } from "vitest";
import { describePublishedYears, periodYear, publishedYears } from "../../../apps/web/lib/publishedYears";

describe("published years", () => {
  it("names a period by its last year", () => {
    expect(periodYear({ period_start: "2020-01-01", period_end: "2024-12-31" })).toBe(2024);
    expect(periodYear({ period_start: "2024-07-01", period_end: null })).toBe(2024);
    expect(periodYear({ period_start: "2024-03-01" })).toBe(2024);
    expect(periodYear({ period_start: "" })).toBeNull();
  });

  it("counts distinct years, not periods or loads", () => {
    const months = Array.from({ length: 24 }, (_, index) => ({
      period_start: `${2023 + Math.floor(index / 12)}-${String((index % 12) + 1).padStart(2, "0")}-01`,
    }));
    expect(publishedYears(months)).toEqual([2023, 2024]);
    expect(describePublishedYears(months)).toBe("2 years published, 2023–2024");
  });

  it("reads the SAIPE case as two years", () => {
    const saipe = [
      { period_start: "2024-01-01", period_end: "2024-12-31" },
      { period_start: "2023-01-01", period_end: "2023-12-31" },
    ];
    expect(describePublishedYears(saipe)).toBe("2 years published, 2023–2024");
  });

  it("says one year, gaps and a cut-short listing plainly", () => {
    expect(describePublishedYears([{ period_start: "2024-01-01", period_end: "2024-12-31" }])).toBe(
      "1 year published, 2024",
    );
    expect(
      describePublishedYears([
        { period_start: "2010-01-01" },
        { period_start: "2015-01-01" },
        { period_start: "2016-01-01" },
      ]),
    ).toBe("3 years published, between 2010 and 2016, with gaps");
    expect(describePublishedYears([{ period_start: "2024-01-01" }], false)).toBe(
      "at least 1 year published, 2024; the list is incomplete",
    );
    expect(describePublishedYears([])).toBe("no published years");
  });
});
