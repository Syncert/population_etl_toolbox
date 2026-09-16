import { describe, expect, test } from "vitest";

// Covers: WEB-108 — a shared link carries a title that says what it shows,
// and nothing outside the public URL vocabulary ever reaches one.

import {
  MAX_TITLED_SERIES,
  STATIC_ROUTE_TITLES,
  comparisonTitle,
  explorerTitle,
  profileTitle,
  searchString,
  workbenchTitle,
} from "../../../apps/web/lib/routeTitles";

describe("titles built from the public URL state", () => {
  test("the explorer names its source, measure and geography", () => {
    expect(
      explorerTitle("source=census&metric=CENSUS_ACS:acs5:B01003_001&geo_level=COUNTY"),
    ).toBe("Explore: census · CENSUS_ACS:acs5:B01003_001 · COUNTY");
  });

  test("a bare route keeps the screen's own name", () => {
    // Not "Explore: undefined", and not the site's default title either: an
    // address with no state still says which screen it is.
    expect(explorerTitle("")).toBe("Explore");
    expect(comparisonTitle(undefined)).toBe("Compare");
    expect(workbenchTitle(null)).toBe("Workbench");
    expect(profileTitle({})).toBe("Place profile");
  });

  test("the comparison names both measures, in the order the URL carries", () => {
    expect(
      comparisonTitle("a=CENSUS_ACS:acs5:B01003_001&b=FRED:UNRATE&geo_level=STATE"),
    ).toBe("Compare: CENSUS_ACS:acs5:B01003_001 against FRED:UNRATE · STATE");
  });

  test("a workbench composition counts out past the second series", () => {
    // Eight series is a legitimate composition and an illegible title: every
    // surface truncates it, and the truncation lands in an arbitrary place.
    const series = (code) => `s=${encodeURIComponent(`src:fred;m:FRED:${code}`)}`;
    const many = ["A", "B", "C", "D"].map(series).join("&");
    const title = workbenchTitle(many);
    expect(title).toContain("FRED:A");
    expect(title).toContain("FRED:B");
    expect(title).toContain(`and ${4 - MAX_TITLED_SERIES} more`);
    expect(title).not.toContain("FRED:D");
  });

  test("a key outside the vocabulary never reaches a title", () => {
    // A title is copied into a bookmark, read aloud, sent to a link
    // previewer and written to browser history. The builders read the parsed
    // state rather than the query, so a key `urlState.ts` does not define
    // has nowhere to arrive from -- asserted rather than assumed, because
    // this is the one rule whose failure leaks something.
    const hostile = [
      "value=561504",
      "token=secret-token",
      "name=Q3%20headcount%20review",
      "analysis_id=42",
      "email=someone%40example.org",
      "source=census",
    ].join("&");

    for (const title of [
      explorerTitle(hostile),
      comparisonTitle(hostile),
      profileTitle(hostile),
      workbenchTitle(hostile),
    ]) {
      expect(title).not.toMatch(/561504|secret-token|headcount|example\.org/);
      expect(title).not.toMatch(/\b42\b/);
    }

    // The one key in that URL the vocabulary does define is still read, so
    // the assertion above is not passing because nothing was parsed at all.
    expect(explorerTitle(hostile)).toBe("Explore: census");
  });

  test("a malformed value in a known key is dropped, not rendered", () => {
    // `urlState.ts` validates each key it knows. A source key with a space,
    // a state FIPS that is not two digits: the parser refuses them, so the
    // title carries neither.
    expect(explorerTitle("source=not a key&state=6")).toBe("Explore");
  });

  test("every static route has a title and none is the site default", () => {
    for (const [route, title] of Object.entries(STATIC_ROUTE_TITLES)) {
      expect(title, route).toBeTruthy();
      if (route !== "/") {
        expect(title, route).not.toBe("Economic Data Studio");
      }
    }
  });

  test("a query object and a query string are the same title", () => {
    // `generateMetadata` is handed an object; the parsers take a string.
    expect(searchString({ source: "census", metric: "FRED:UNRATE" })).toBe(
      "source=census&metric=FRED%3AUNRATE",
    );
    expect(searchString({ s: ["src:fred;m:FRED:A", "src:fred;m:FRED:B"] })).toContain("s=");
    expect(explorerTitle({ source: "census" })).toBe(explorerTitle("source=census"));
  });
});
