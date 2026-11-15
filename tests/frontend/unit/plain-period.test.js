// Covers: WEB-177 — a served period is written in the reader's words, never
// reworded into a different span (website UX review 2026-11, B1).

import { describe, expect, it } from "vitest";

import { plainPeriodStart, plainPeriods } from "../../../apps/web/lib/plainPeriod";

describe("a published period in the reader's words", () => {
  it("writes whole years as years", () => {
    expect(plainPeriods("2024-01-01 – 2024-12-31")).toBe("2024");
    expect(plainPeriods("2020-01-01 – 2024-12-31")).toBe("2020–2024");
  });

  it("writes whole months as months, within a year and across years", () => {
    expect(plainPeriods("2026-09-01 – 2026-09-30")).toBe("September 2026");
    expect(plainPeriods("2024-02-01 – 2024-02-29")).toBe("February 2024");
    expect(plainPeriods("2026-01-01 – 2026-03-31")).toBe("January–March 2026");
    expect(plainPeriods("2026-10-01 – 2027-09-30")).toBe("October 2026 – September 2027");
  });

  it("keeps both dates of a span that is not whole months, and a single day, in words", () => {
    expect(plainPeriods("2025-07-01")).toBe("July 1, 2025");
    expect(plainPeriods("2024-11-05 – 2024-11-05")).toBe("November 5, 2024");
    expect(plainPeriods("2020-03-29 – 2020-04-04")).toBe("March 29, 2020 – April 4, 2020");
    // February 28 is not the last day of a leap February.
    expect(plainPeriods("2024-02-01 – 2024-02-28")).toBe("February 1, 2024 – February 28, 2024");
  });

  it("rewrites every period inside a longer text and leaves the rest alone", () => {
    expect(plainPeriods("Not published for 2024-01-01 – 2024-12-31 (newest published: 2023-01-01 – 2023-12-31)"))
      .toBe("Not published for 2024 (newest published: 2023)");
    expect(plainPeriods("2025-07-01; 2020-01-01 – 2024-12-31")).toBe("July 1, 2025; 2020–2024");
    expect(plainPeriods("2020–2024")).toBe("2020–2024");
    expect(plainPeriods("Reading…")).toBe("Reading…");
    expect(plainPeriods(null)).toBe("");
  });

  it("names the period a series begins in from its first day", () => {
    expect(plainPeriodStart("2020-01-01")).toBe("2020");
    expect(plainPeriodStart("2025-07-01")).toBe("July 2025");
    expect(plainPeriodStart("2024-11-05")).toBe("November 5, 2024");
    expect(plainPeriodStart("2023")).toBe("2023");
  });
});
