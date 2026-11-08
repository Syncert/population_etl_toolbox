// Covers: WEB-162 — a headline card draws its levels on one scale, with margins,
// and words the comparison in the record page verbs (ADR-0011).

import { describe, expect, it } from "vitest";

import { forbiddenWordsIn } from "../../../apps/web/lib/checkTheRecord";
import { COUNT_MEASURES, levelStrip, stripSentence, stripTicks } from "../../../apps/web/lib/levelStrip";

const place = (name, level, geoId) => ({ name, level, geoId, role: level === "COUNTY" ? "this place" : "parent" });
const row = (value, margin) => ({ value, margin_of_error: margin ?? null });
const card = (...entries) => entries.map(([name, level, value, margin, message = ""]) => ({
  place: place(name, level, name),
  row: value === null ? null : row(value, margin),
  message,
}));

describe("levelStrip", () => {
  it("places the county, its state and the nation on one scale with margins", () => {
    const strip = levelStrip("median-household-income", card(
      ["Dane County", "COUNTY", 89975, 1284],
      ["Wisconsin", "STATE", 77485, 455],
      ["United States", "NATION", 80734, 166],
    ));
    expect(strip.points.map((point) => point.margin)).toEqual([1284, 455, 166]);
    expect(strip.domain[0]).toBeLessThan(77485 - 455);
    expect(strip.domain[1]).toBeGreaterThan(89975 + 1284);
    expect(strip.sentence).toBe("Dane County was higher than Wisconsin and the United States at the published margins of error.");
    expect(forbiddenWordsIn(strip.sentence)).toEqual([]);
  });

  it("says which way a vote margin leans, so a Democratic county is not read as plain 'higher'", () => {
    // A margin is the Republican minus the Democratic share (ADR-0014).
    const strip = levelStrip("president-margin", card(
      ["Dane County", "COUNTY", -53.48, null],
      ["Wisconsin", "STATE", 0.86, null],
    ));
    expect(strip.domain[0]).toBeLessThan(-53.48);
    expect(strip.sentence).toBe(
      "Dane County was lower than Wisconsin. A higher margin is further Republican, a lower one further Democratic.",
    );
    expect(forbiddenWordsIn(strip.sentence)).toEqual([]);
  });

  it("calls overlapping survey intervals not distinguishable", () => {
    const strip = levelStrip("median-age", card(
      ["Dane County", "COUNTY", 35.7, 0.3],
      ["Wisconsin", "STATE", 40.2, 0.1],
      ["United States", "NATION", 35.8, 0.1],
    ));
    expect(strip.sentence).toBe(
      "Dane County was lower than Wisconsin and was not distinguishable from the United States at the published margins of error.",
    );
  });

  it("leaves out a level the margin rule cannot be applied to", () => {
    expect(stripSentence([
      { name: "Dane County", value: 10, margin: 1, index: 0 },
      { name: "Wisconsin", value: 5, margin: null, index: 1 },
    ])).toBe("");
  });

  it("compares published values directly when no level carries a margin", () => {
    const strip = levelStrip("unemployment-rate", card(
      ["Madison, WI", "METRO", 2.5],
      ["Wisconsin", "STATE", 2.9],
    ));
    expect(strip.sentence).toBe("Madison, WI was lower than Wisconsin.");
    expect(strip.domain[0]).toBeGreaterThanOrEqual(0);
  });

  it("draws no strip for a count, or with fewer than two published values", () => {
    for (const id of COUNT_MEASURES) {
      expect(levelStrip(id, card(["A", "COUNTY", 1], ["B", "STATE", 2]))).toBeNull();
    }
    expect(levelStrip("median-age", card(
      ["Dane County", "COUNTY", null, null, "Not published for this place"],
      ["Wisconsin", "STATE", 40.2, 0.1],
    ))).toBeNull();
  });

  it("states parents only when the page's place has no value", () => {
    const strip = levelStrip("median-age", card(
      ["Dane County", "COUNTY", null, null, "Not published for this place"],
      ["Wisconsin", "STATE", 40.2, 0.1],
      ["United States", "NATION", 38.9, 0.1],
    ));
    expect(strip.points).toHaveLength(2);
    expect(strip.sentence).toBe("");
  });
});

describe("stripTicks", () => {
  it("returns round ticks inside the domain", () => {
    expect(stripTicks(73000, 94000)).toEqual([75000, 80000, 85000, 90000]);
    expect(stripTicks(0, 3.4)).toEqual([0, 1, 2, 3]);
  });
});
