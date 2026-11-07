import React from "react";

// Covers: WEB-152 — the age and sex section's model and charts
// (web-age-and-sex-structure, ADR-0013): the toggle's choices in the API's
// order with None and the default, the difference bars as served share minus
// served share, unreliable bands hatched and absent bands said (never a
// zero-length bar), cohort sentences limited to ADR-0011's verbs, and no
// single B01001 line offered as a map. Fixtures are the contract's shape.
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import AgePyramid, { AgePyramidTable } from "../../../apps/web/components/AgePyramid";
import AgeSummaryCards from "../../../apps/web/components/AgeSummaryCards";
import CohortChart from "../../../apps/web/components/CohortChart";
import {
  NO_REFERENCE,
  bandDifferences,
  cohortFromRelease,
  cohortSentence,
  defaultReferenceKey,
  earlierReleases,
  nameAgeHistory,
  nameAgeStructure,
  readableName,
  num,
  overlapText,
  placeOf,
  pyramidExtent,
  referenceChoices,
  referenceFor,
  signedPoints,
} from "../../../apps/web/lib/ageStructure";
import { isSingleAgeLine } from "../../../apps/web/lib/ageLines";
import { DANE_TRACTS } from "../support/placeScenarios.js";
import { ageBlock, ageCohortsAnswer, ageHistoryAnswer, ageStructureAnswer as rawAnswer } from "../support/ageScenarios.js";

const MADISON = "state:55|place:48000";
const DANE = "state:55|county:025";
const TRACT = DANE_TRACTS[0].geo_id;
// As the client holds an answer: named for a reader where it arrives.
const ageStructureAnswer = (...args) => nameAgeStructure(rawAnswer(...args));

describe("names", () => {
  test("the nation is never shown by its id, nor a place by its id", () => {
    // The catalog names the nation "us:1" (OFFICE_LAB, 2026-10-31).
    expect(rawAnswer(MADISON).places.at(-1).geo_name).toBe("us:1");
    expect(referenceChoices(ageStructureAnswer(MADISON)).map((choice) => choice.name)).toEqual([
      "Dane County, Wisconsin", "Wisconsin", "United States", "",
    ]);
    expect(nameAgeStructure(rawAnswer("us:1")).geo_name).toBe("United States");
    expect(nameAgeHistory(ageHistoryAnswer("us:1")).items.every((item) => item.geo_name === "United States")).toBe(true);
    expect(readableName({ geo_id: "state:55|place:1", geo_name: "state:55|place:1" })).toBe("this place");
    expect(readableName({ geo_id: "state:55", geo_name: "Wisconsin" })).toBe("Wisconsin");
  });
});

describe("the comparison toggle", () => {
  test("a city offers county, state, nation and none, county selected", () => {
    const answer = ageStructureAnswer(MADISON);
    expect(referenceChoices(answer).map((choice) => choice.levelWord)).toEqual(["County", "State", "Nation", "None"]);
    expect(defaultReferenceKey(answer)).toBe(DANE);
    expect(referenceFor(answer, NO_REFERENCE)).toBeNull();
    expect(referenceFor(answer, "us:1")?.geo_name).toBe("United States");
  });

  test("a county offers state, nation and none; the nation offers nothing", () => {
    expect(referenceChoices(ageStructureAnswer(DANE)).map((choice) => choice.levelWord)).toEqual(["State", "Nation", "None"]);
    expect(referenceChoices(ageStructureAnswer("us:1"))).toEqual([]);
    expect(defaultReferenceKey(ageStructureAnswer("us:1"))).toBe(NO_REFERENCE);
  });

  test("the county option names its overlap only below 100 %", () => {
    const crossing = ageStructureAnswer("state:55|place:99999");
    const county = referenceChoices(crossing)[0];
    expect(county.overlapNote).toBe("60% of Crossing city, Wisconsin's area lies in Dane County, Wisconsin");
    expect(referenceChoices(ageStructureAnswer(MADISON))[0].overlapNote).toBeNull();
    // 99.99 % never reads as 100 %.
    expect(overlapText(placeOf(crossing), { ...county.place, overlap_weight: 0.99999 })).toMatch(/^99\.9%/);
  });

  test("an API default that is not a listed reference falls back to the first", () => {
    const answer = { ...ageStructureAnswer(MADISON), default_reference_geo_id: "state:99" };
    expect(defaultReferenceKey(answer)).toBe(DANE);
  });
});

describe("numbers are the API's", () => {
  test("a null is never zero", () => {
    expect(num(null)).toBeNull();
    expect(num("")).toBeNull();
    expect(num("0")).toBe(0);
  });

  test("the difference is served share minus served share, null when either is missing", () => {
    const place = ageBlock(TRACT);
    const reference = ageBlock(DANE, { role: "reference" });
    const rows = bandDifferences(place, reference);
    expect(rows).toHaveLength(18);
    expect(rows[5].men).toBeCloseTo(Number(place.bands[5].men.share) - Number(reference.bands[5].men.share), 10);
    expect(rows[17].women).toBeNull();
    expect(signedPoints(-0.5)).toBe("−0.50");
    expect(signedPoints(0.004)).toBe("0.00");
  });

  test("the shared axis reaches the widest share plus its margin", () => {
    const a = ageBlock(MADISON);
    const widest = Math.max(...a.bands.flatMap((band) => ["men", "women"].map((sex) => Number(band[sex].share) + Number(band[sex].share_moe))));
    expect(pyramidExtent([a, null])).toBe(Math.ceil(widest));
  });

  test("an earlier outline is any other drawable release; cohorts start five years back", () => {
    const history = ageHistoryAnswer(DANE, { includeBands: true });
    expect(earlierReleases(history, "2023").map((item) => item.release)).toEqual(["2018", "2013"]);
    expect(earlierReleases(ageHistoryAnswer(DANE), "2023")).toEqual([]);
    expect(cohortFromRelease("2023")).toBe("2018");
    expect(cohortFromRelease(null)).toBeNull();
  });
});

describe("the pyramid", () => {
  test("eighteen bands per side, with the reference outlined", () => {
    render(<AgePyramid place={ageBlock(DANE)} reference={ageBlock("state:55", { role: "reference" })} testId="p" />);
    expect(document.querySelectorAll("[data-testid^='p-bar-']")).toHaveLength(36);
    expect(screen.getByTestId("p-reference-men")).toBeInTheDocument();
    expect(screen.getByTestId("p-legend-reference")).toHaveTextContent("Wisconsin, 2019–2023");
  });

  test("an unreliable band is hatched and named; an absent band is said, never a bar", () => {
    render(<AgePyramid place={ageBlock(TRACT)} testId="p" />);
    expect(screen.getByTestId("p-bar-80-84-men")).toHaveAttribute("data-state", "unreliable");
    expect(screen.getByTestId("p-legend-unreliable")).toHaveTextContent("unreliable");
    expect(screen.getByTestId("p-hit-80-84-men").getAttribute("aria-label")).toMatch(/unreliable/);
    const absent = screen.getByTestId("p-bar-85+-women");
    expect(absent).toHaveAttribute("data-state", "absent");
    expect(absent.tagName.toLowerCase()).toBe("text");
    expect(absent.querySelector("path")).toBeNull();
    expect(screen.getByTestId("p-hit-85+-women").getAttribute("aria-label")).toBe(
      "Women aged 85 and over: the Census Bureau withheld one of the published lines this group is made of.",
    );
  });

  test("the table carries the same numbers, oldest first", () => {
    const place = ageBlock(DANE);
    render(<AgePyramidTable place={place} testId="t" />);
    const rows = screen.getByTestId("t").querySelectorAll("tbody tr");
    expect(rows).toHaveLength(18);
    expect(rows[0].querySelector("th")).toHaveTextContent("85 and over");
    expect(rows[17]).toHaveTextContent(Number(place.bands[0].men.count).toLocaleString("en-US"));
  });
});

describe("the cards", () => {
  test("computed figures carry the draft mark; the published median never does", () => {
    const answer = ageStructureAnswer(MADISON);
    render(<AgeSummaryCards place={placeOf(answer)} reference={referenceFor(answer, DANE)} scheme={answer.scheme} testId="c" />);
    expect(screen.getByTestId("c-median-age-value")).toHaveTextContent("31.6");
    expect(screen.getByTestId("c-median-age-reference")).toHaveTextContent("Dane County, Wisconsin: 35.4");
    expect(screen.queryByTestId("c-median-age-draft")).toBeNull();
    expect(screen.getByTestId("c-sex-ratio-draft")).toHaveTextContent("draft");
  });

  test("no comparison line when None is chosen", () => {
    const answer = ageStructureAnswer(MADISON);
    render(<AgeSummaryCards place={placeOf(answer)} reference={null} scheme={{ ...answer.scheme, status: "approved" }} testId="c" />);
    expect(screen.queryByTestId("c-median-age-reference")).toBeNull();
    expect(screen.queryByTestId("c-sex-ratio-draft")).toBeNull();
  });
});

describe("cohorts", () => {
  test("sentences use only ADR-0011's verbs and the uncertainty decides", () => {
    const answer = ageCohortsAnswer(DANE, "2018", "2023");
    const item = answer.items[4];
    const overlapping = cohortSentence({ ...item, men: { ...item.men, intervals_overlap: true } }, "men", answer.from, answer.to);
    expect(overlapping).toContain("was not distinguishable from it");
    const rose = cohortSentence({ ...item, men: { ...item.men, change: "500", intervals_overlap: false } }, "men", answer.from, answer.to);
    expect(rose).toMatch(/^From men aged 20 to 24 in 2014–2018 to men aged 25 to 29 in 2019–2023, the count rose/);
    const fell = cohortSentence({ ...item, men: { ...item.men, change: "-500", intervals_overlap: false } }, "men", answer.from, answer.to);
    expect(fell).toContain("fell");
    // A missing margin leaves the overlap unknown, which never earns "rose" or "fell".
    const unknown = cohortSentence({ ...item, men: { ...item.men, change: "500", intervals_overlap: null } }, "men", answer.from, answer.to);
    expect(unknown).toContain("was not distinguishable from it");
    for (const sentence of [overlapping, rose, fell]) {
      expect(sentence).not.toMatch(/sharply|dramatically|because|due to|driven by|better|worse/);
    }
  });

  test("a refused pair is said in words and nothing is drawn", () => {
    render(<CohortChart cohorts={ageCohortsAnswer(DANE, "2019", "2023")} testId="k" />);
    expect(screen.getByTestId("k-refusal")).toHaveAttribute("data-reason", "overlapping_periods");
    expect(screen.getByTestId("k-refusal")).toHaveTextContent("share years");
    expect(document.querySelector("svg")).toBeNull();
  });
});

describe("the one-measure map", () => {
  test("offers no single B01001 age line, but still maps the totals", () => {
    expect(isSingleAgeLine("CENSUS_ACS:acs5:B01001_003")).toBe(true);
    expect(isSingleAgeLine("CENSUS_ACS:acs1:B01001_049")).toBe(true);
    expect(isSingleAgeLine("CENSUS_ACS:acs5:B01001_001")).toBe(false);
    expect(isSingleAgeLine("CENSUS_ACS:acs5:B01001_002")).toBe(false);
    expect(isSingleAgeLine("CENSUS_ACS:acs5:B01001_026")).toBe(false);
    expect(isSingleAgeLine("CENSUS_ACS:acs5:B01002_001")).toBe(false);
  });
});
