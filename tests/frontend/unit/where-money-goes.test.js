import { describe, expect, it } from "vitest";

import {
  EVERYTHING_ELSE,
  NOT_IN_A_LINE,
  WHERE_MONEY_GEO_ID,
  dollars,
  layerHeading,
  moneyText,
  shareBars,
  shareText,
  totalSentence,
  unsharedReason,
} from "../../../apps/web/lib/whereMoneyGoes";

// Covers: WEB-176 — where-public-money-goes slice 5: each government level's
// spending lines as shares of that level's own total, the largest drawn and
// the rest one "Everything else" bar of the same total, Census thousands read
// as dollars, a missing amount "not published" (never zero), no share drawn
// where none was published, and no layer added to another.

const line = (key, amount, share, reason = null) => ({ key, label: key, amount, share_pct: share, unavailable_reason: reason });
const layer = (lines, overrides = {}) => ({
  layer: "state_local_spending",
  title: "What local governments in the county area spend",
  spender: "local governments in the county area",
  source: "Census of Governments",
  geo_id: "state:55|county:025",
  fiscal_year: 2022,
  period_label: "fiscal year ending in 2022",
  unit: "thousands of US dollars",
  total: "2000",
  total_label: "Direct general spending",
  lines,
  notes: [],
  ...overrides,
});

describe("where the money goes", () => {
  it("answers only the nation, a state or a county", () => {
    expect(WHERE_MONEY_GEO_ID.test("us:1")).toBe(true);
    expect(WHERE_MONEY_GEO_ID.test("state:55")).toBe(true);
    expect(WHERE_MONEY_GEO_ID.test("state:55|county:025")).toBe(true);
    expect(WHERE_MONEY_GEO_ID.test("state:55|place:48000")).toBe(false);
    expect(WHERE_MONEY_GEO_ID.test("cbsa:31540")).toBe(false);
  });

  it("reads Census thousands as dollars and writes amounts a reader scans", () => {
    expect(dollars("1405622", "thousands of US dollars")).toBe(1405622000);
    expect(dollars("2978864182.74", "US dollars")).toBe(2978864182.74);
    expect(dollars(null, "US dollars")).toBeNull();
    expect(moneyText(1405622000)).toBe("$1.41 billion");
    expect(moneyText(7395799983313.23)).toBe("$7.40 trillion");
    expect(moneyText(112441461673.81)).toBe("$112 billion");
    expect(moneyText(853504)).toBe("$853,504");
    expect(moneyText(-5000000)).toBe("−$5.00 million");
    expect(moneyText(null)).toBe("not published");
    expect(shareText("75.76")).toBe("75.8%");
    expect(shareText(null)).toBe("not published");
  });

  it("draws the largest lines and the rest of the same total as one bar", () => {
    const lines = [
      line("a", "900", "45.00"),
      line("b", "400", "20.00"),
      line("c", "200", "10.00"),
      line("d", "150", "7.50"),
      line("e", "100", "5.00"),
      line("f", "100", "5.00"),
      line("g", "90", "4.50"),
      line("h", "60", "3.00"),
    ];
    const bars = shareBars(layer(lines));
    expect(bars.map((bar) => bar.key)).toEqual(["a", "b", "c", "d", "e", "f", "rest"]);
    const rest = bars.at(-1);
    expect(rest.label).toBe(EVERYTHING_ELSE);
    expect(rest.share).toBe(7.5);
    // The rest of Census's own total, in dollars: 2,000 less the six shown, in thousands.
    expect(rest.amount).toBe(150000);
  });

  it("draws no bar for a negative or missing line, and no rest when the lines hold the whole total", () => {
    const bars = shareBars(
      layer([line("grants", "70", "70.00"), line("contracts", "30", "30.00"), line("loans", "-5", "-5.00"), line("fire", null, null, "suppressed_input")], { unit: "US dollars", total: "100" }),
    );
    expect(bars.map((bar) => bar.key)).toEqual(["grants", "contracts"]);
  });

  it("names what no line holds when every line is drawn (an agency USAspending did not name)", () => {
    const agencies = layer([line("HHS", "60", "60.00"), line("DOD", "30", "30.00")], { layer: "federal_awards_by_agency", unit: "US dollars", total: "100" });
    const bars = shareBars(agencies);
    expect(bars.map((bar) => [bar.label, bar.share, bar.amount])).toEqual([
      ["HHS", 60, 60],
      ["DOD", 30, 30],
      [NOT_IN_A_LINE, 10, 10],
    ]);
  });

  it("draws nothing where no share was published and says why once", () => {
    const unshared = layer(
      [line("grants", "2978864182.74", null, "all-award total not published for this place and year")],
      { layer: "federal_awards_by_type", unit: "US dollars", total: null, total_label: "All federal award obligations recorded here" },
    );
    expect(shareBars(unshared)).toEqual([]);
    expect(unsharedReason(unshared)).toBe("all-award total not published for this place and year");
    expect(totalSentence(unshared)).toBeNull();
  });

  it("names the layer's year and total in words", () => {
    const shown = layer([line("a", "2000", "100.00")]);
    expect(layerHeading(shown)).toBe("What local governments in the county area spend, fiscal year ending in 2022");
    expect(totalSentence(shown)).toBe("Direct general spending: $2.00 million.");
    expect(layerHeading({ ...shown, period_label: null })).toBe("What local governments in the county area spend");
  });
});
