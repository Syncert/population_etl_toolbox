import { describe, expect, test } from "vitest";

// Covers: WEB-150 — a county crime rate computed from agency reports
// (ADR-0012) stands in the FBI rate card's county row, never as a view of a
// count; whom it covers is worded beside the figure and the agencies behind it
// are counted for the tooltip; a refused month reads as its reason.

import {
  agenciesText,
  agencyRateFor,
  coverageText,
  derivedValueText,
  pairingFor,
  viewChoices,
} from "../../../apps/web/lib/derivedViews.ts";
import { unitText } from "../../../apps/web/lib/explorerViewModel.ts";

const VIOLENT_COUNT = "FBI_UCR:summarized_violent_crime:V:offense:absolute_total";
const VIOLENT_RATE = "FBI_UCR:summarized_violent_crime:V:offense:rate";
const AGENCY = {
  ratio_id: "violent_crime_county_agency_rate", kind: "agency_rate", derived: true, label: "Violent crime per 100,000 people, computed from agency reports",
  unit: "per 100,000 people covered by reporting agencies in this county", scale: 100000, numerator: VIOLENT_COUNT, denominator: null, provider_rate: null,
  reads_beside: VIOLENT_RATE, geo_levels: ["COUNTY"], denominator_label: "FBI population of the agencies serving only this county that reported",
  universe: "people covered", status: "draft", version: 1, served: true, refusal_reason: null, limitations: [],
};
const coverage = (overrides) => ({
  covered_population: "115648", linked_population: "115648", coverage_share: "1.0000", coverage_floor: "0.5",
  contributing_agencies: ["WI0130000", "WI0137000"], excluded_multi_county_agencies: ["WI0540300"], non_reporting_agencies: [], unknown_population_agencies: [], ...overrides,
});

describe("county agency rate", () => {
  test("it stands beside the FBI rate and is never a view of the count", () => {
    expect(agencyRateFor([AGENCY], VIOLENT_RATE)).toBe(AGENCY);
    expect(agencyRateFor([{ ...AGENCY, served: false }], VIOLENT_RATE)).toBeNull();
    expect(agencyRateFor([AGENCY], VIOLENT_COUNT)).toBeNull();
    expect(pairingFor([AGENCY], VIOLENT_COUNT, "per_capita")).toBeNull();
    expect(viewChoices([AGENCY], [VIOLENT_COUNT])).toEqual(["count"]);
  });

  test("coverage is worded in people and share, rounded down", () => {
    expect(coverageText(coverage({}))).toBe("covers 115,648 people, 100 % of the population its agencies serve");
    // 49.99 % is under the floor; it must never read as 50 %.
    expect(coverageText(coverage({ covered_population: "9000", coverage_share: "0.4999" }))).toBe("covers 9,000 people, 49 % of the population its agencies serve");
    expect(coverageText(coverage({ covered_population: "1", coverage_share: null }))).toBe("covers 1 person");
    expect(coverageText(coverage({ covered_population: null, coverage_share: null }))).toBe("no reporting agency serving only this county published a population");
    expect(coverageText(null)).toBe("");
    // Unloaded agencies leave coverage unjudged; the count says so.
    expect(coverageText(coverage({ coverage_share: null, not_loaded_agencies: ["WI0130100"] }))).toBe("covers 115,648 people; 1 agency serving this county is not loaded yet");
    expect(coverageText(coverage({ coverage_share: null, not_loaded_agencies: ["A", "B"] }))).toBe("covers 115,648 people; 2 agencies serving this county are not loaded yet");
  });

  test("the agencies behind it are counted for the tooltip", () => {
    expect(agenciesText(coverage({}))).toBe("2 reporting agencies; 1 agency left out because it also serves other counties");
    expect(agenciesText(coverage({ contributing_agencies: ["A"], non_reporting_agencies: ["B", "C"], excluded_multi_county_agencies: [], unknown_population_agencies: ["D"] })))
      .toBe("1 reporting agency; 2 agencies that did not report; 1 agency left out with no published population");
    expect(agenciesText(coverage({ not_loaded_agencies: ["WI0130100"] })))
      .toBe("2 reporting agencies; 1 agency left out because it also serves other counties; 1 agency whose reports are not loaded");
  });

  test("a refused month reads as its reason, never as zero", () => {
    const base = { ratio_id: AGENCY.ratio_id, derived: true, pairing_status: "draft", pairing_version: 1, unit: AGENCY.unit, scale: 100000, margin_of_error_method: "none" };
    expect(derivedValueText({ ...base, value: "11.24", unavailable_reason: null })).toBe("11.24 per 100,000 people covered by reporting agencies in this county");
    expect(derivedValueText({ ...base, value: null, unavailable_reason: "coverage_below_floor" })).toBe("Too few agencies reported to give a rate");
    expect(derivedValueText({ ...base, value: null, unavailable_reason: "no_reporting_agency" })).toBe("No agency serving this county reported this month");
    expect(derivedValueText({ ...base, value: null, unavailable_reason: "only_multi_county_agencies" })).toBe("Only agencies that also serve other counties reported");
    expect(derivedValueText({ ...base, value: null, unavailable_reason: "agencies_not_loaded" })).toBe("Reports from some agencies serving this county are not loaded yet");
  });

  test("the FBI's unit code reads in words", () => {
    expect(unitText("per_100000_population")).toBe("per 100,000 people");
    expect(unitText("dollars")).toBe("dollars");
    expect(unitText(null)).toBe("");
  });
});
