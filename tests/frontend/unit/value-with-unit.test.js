// Covers: WEB-179 — money and percentages are written the way a reader
// writes them; the number itself is never changed.
import { describe, expect, it } from "vitest";

import { valueWithUnit } from "../../../apps/web/lib/explorerViewModel";

describe("a value with its unit, in a reader's words", () => {
  it("writes dollars with the sign, keeping what the price is per", () => {
    expect(valueWithUnit("89,975", "Dollars")).toBe("$89,975");
    expect(valueWithUnit("89,975", "US dollars")).toBe("$89,975");
    expect(valueWithUnit("1,631", "dollars per week")).toBe("$1,631 per week");
    expect(valueWithUnit("-412", "dollars")).toBe("-$412");
  });

  it("writes a percentage with the sign, keeping what it is a share of", () => {
    expect(valueWithUnit("9.9", "percent")).toBe("9.9%");
    expect(valueWithUnit("4.1", "percent of personal income")).toBe("4.1% of personal income");
  });

  it("says people for persons and leaves every other unit as published", () => {
    expect(valueWithUnit("590,375", "persons")).toBe("590,375 people");
    expect(valueWithUnit("35.7", "Years")).toBe("35.7 Years");
    expect(valueWithUnit("51,263,648", "thousands of US dollars")).toBe("51,263,648 thousands of US dollars");
    expect(valueWithUnit("1.2", "percentage points")).toBe("1.2 percentage points");
    expect(valueWithUnit("12.4", "per 1,000 people")).toBe("12.4 per 1,000 people");
  });

  it("adds nothing to a missing value or a missing unit", () => {
    expect(valueWithUnit("-", "Dollars")).toBe("-");
    expect(valueWithUnit("42", "")).toBe("42");
    expect(valueWithUnit("42", "value")).toBe("42");
  });
});
