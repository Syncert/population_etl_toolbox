import { describe, expect, test } from "vitest";

// Covers: WEB-145 — per-capita and share views (web-per-capita-toggle,
// ADR-0009): the toggle offers only the kinds a served pairing divides a
// count by, a provider's own rate is a per-capita view, the denominator is
// named beside every value, a value the API could not compute reads as the
// API's reason and never as zero, and a draft pairing is marked as one.

import {
  choiceLabel,
  denominatorText,
  derivedMapRow,
  derivedMarginText,
  derivedValueText,
  isDraft,
  pairingFor,
  providerRateFor,
  viewChoices,
  viewKey,
} from "../../../apps/web/lib/derivedViews.ts";

const pairing = (overrides) => ({
  ratio_id: "poverty_share:acs5", kind: "share", derived: true, label: "Share of people below the poverty level", unit: "percent", scale: 100,
  numerator: "CENSUS_ACS:acs5:B17001_002", denominator: "CENSUS_ACS:acs5:B17001_001", provider_rate: null, dataset: "acs5",
  denominator_label: "people whose poverty status is determined", universe: "people whose poverty status is determined",
  numerator_is_subset: true, status: "draft", version: 1, served: true, refusal_reason: null, limitations: [], ...overrides,
});
const POVERTY = pairing({});
const BIRTHS = pairing({ ratio_id: "birth_rate", kind: "provider_rate", derived: false, unit: "per 1,000 residents", scale: 1000, numerator: "CENSUS_PEP:BIRTHS",
  denominator: null, provider_rate: "CENSUS_PEP:RBIRTH", denominator_label: "average resident population over the estimate year (PEP)" });
const VIOLENT = pairing({ ratio_id: "violent_crime_rate", kind: "provider_rate", derived: false, unit: "per 100,000 people", scale: 100000,
  numerator: "FBI_UCR:summarized_violent_crime:V:offense:absolute_total", denominator: null, provider_rate: "FBI_UCR:summarized_violent_crime:V:offense:rate",
  denominator_label: "population covered by reporting agencies", served: false, refusal_reason: "pairing is draft" });

describe("which views a count offers", () => {
  test("only served pairings, matched on the numerator's exact code", () => {
    const pairings = [POVERTY, BIRTHS, VIOLENT];
    expect(pairingFor(pairings, "CENSUS_ACS:acs5:B17001_002", "share")).toBe(POVERTY);
    expect(pairingFor(pairings, "CENSUS_ACS:acs1:B17001_002", "share")).toBeNull();
    expect(pairingFor(pairings, "CENSUS_ACS:acs5:B17001_002", "per_capita")).toBeNull();
    expect(pairingFor(pairings, "CENSUS_PEP:BIRTHS", "per_capita")).toBe(BIRTHS);
    // Not served (a draft without DERIVED_RATIOS_SERVE_DRAFTS): never offered.
    expect(pairingFor(pairings, VIOLENT.numerator, "per_capita")).toBeNull();
    expect(viewChoices(pairings, ["CENSUS_ACS:acs5:B17001_002", "CENSUS_PEP:BIRTHS"])).toEqual(["count", "per_capita", "share"]);
    expect(viewChoices(pairings, ["CENSUS_ACS:acs5:B01003_001"])).toEqual(["count"]);
  });

  test("a provider's rate is found by its own code, served or not, so its denominator is named", () => {
    expect(providerRateFor([POVERTY, VIOLENT], VIOLENT.provider_rate)).toBe(VIOLENT);
    expect(providerRateFor([POVERTY, VIOLENT], POVERTY.numerator)).toBeNull();
  });

  test("the per-capita button takes the API's unit when the pairings agree", () => {
    expect(choiceLabel("per_capita", [BIRTHS])).toBe("Per 1,000 residents");
    expect(choiceLabel("per_capita", [BIRTHS, VIOLENT])).toBe("Per resident");
    expect(choiceLabel("share", [POVERTY])).toBe("Share");
    expect(choiceLabel("count")).toBe("Count");
  });
});

describe("a derived value as it is read", () => {
  const row = (overrides) => ({ ratio_id: POVERTY.ratio_id, derived: true, pairing_status: "draft", pairing_version: 1, geo_id: "state:55|county:025",
    geo_level: "COUNTY", period_start: "2019-01-01", period_end: "2023-12-31", release: "2023", value: "10.42", unit: "percent", scale: 100,
    margin_of_error: "0.81", margin_of_error_method: "proportion", unavailable_reason: null, ...overrides });

  test("names its denominator and the margin's method", () => {
    expect(denominatorText(POVERTY)).toBe("of people whose poverty status is determined");
    expect(denominatorText(VIOLENT)).toBe("denominator: population covered by reporting agencies, as the publisher computed it");
    expect(derivedValueText(row({}))).toBe("10.42 percent");
    expect(derivedMarginText(row({}))).toBe("margin of error ±0.81 (Census Bureau formula for a derived proportion)");
  });

  test("a value the API could not compute is its reason, never zero", () => {
    expect(derivedValueText(row({ value: null, unavailable_reason: "different_period" }))).toBe("No denominator for this period and release");
    expect(derivedValueText(row({ value: null, unavailable_reason: "denominator_zero" }))).toBe("The denominator is zero");
    expect(derivedMarginText(row({ value: null, unavailable_reason: "suppressed_input" }))).toBe("");
    const mapped = derivedMapRow(row({ value: null, unavailable_reason: "suppressed_input" }), viewKey(POVERTY));
    expect(mapped.value).toBeNull();
    expect(mapped.value_status).toBe("An input is withheld or not a usable number");
    expect(mapped.metric_code).toBe("derived:poverty_share:acs5");
  });

  test("only an approved pairing is not a draft", () => {
    expect(isDraft("draft")).toBe(true);
    expect(isDraft("needs_review")).toBe(true);
    expect(isDraft("approved")).toBe(false);
  });
});
