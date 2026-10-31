import { describe, expect, it } from "vitest";

import {
  boundarySentence,
  releaseOptions,
  tractNameFromGeoId,
  uncolouredSentence,
} from "../../../apps/web/components/WithinCounty.tsx";

// Covers: the within-county tract view steps back through published
// releases and says how a boundary redraw shows, instead of silently
// listing today's tracts against an older release.
describe("the releases a county's tracts can be read at", () => {
  it("names each release by the period ending in its year, newest first", () => {
    const options = releaseOptions(
      [{ release: "2019" }, { release: "2024" }, { release: "2022" }],
      [
        { period_start: "2020-01-01", period_end: "2024-12-31", observation_count: 1 },
        { period_start: "2018-01-01", period_end: "2022-12-31", observation_count: 1 },
        { period_start: "2015-01-01", period_end: "2019-12-31", observation_count: 1 },
      ],
    );
    expect(options).toEqual([
      { release: "2024", label: "2020-01-01 – 2024-12-31 (2024 release)" },
      { release: "2022", label: "2018-01-01 – 2022-12-31 (2022 release)" },
      { release: "2019", label: "2015-01-01 – 2019-12-31 (2019 release)" },
    ]);
  });

  it("still offers a release the periods route does not describe", () => {
    expect(releaseOptions([{ release: "2013" }], [])).toEqual([{ release: "2013", label: "2013 release" }]);
    expect(releaseOptions([], [])).toEqual([]);
  });
});

describe("what a boundary redraw says", () => {
  it("is silent when the release is on today's boundaries", () => {
    expect(boundarySentence("2020-01-01 – 2024-12-31", 290, 0, 0)).toBe("");
  });

  it("counts the tracts gone from today's boundaries and today's tracts that did not exist", () => {
    expect(boundarySentence("2015-01-01 – 2019-12-31", 290, 130, 202)).toBe(
      "2015-01-01 – 2019-12-31 is on the tract boundaries of its own release: 130 tracts of that release are not " +
        "in today's boundaries and appear only in the table; 202 of today's 290 tracts did not exist then and have " +
        "no value for it. A value is never carried across a boundary change.",
    );
    expect(boundarySentence("", 10, 1, 0)).toBe(
      "This release is on the tract boundaries of its own release: 1 tract of that release is not in today's " +
        "boundaries and appears only in the table. A value is never carried across a boundary change.",
    );
  });

  it("keeps the uncoloured count about today's tracts", () => {
    expect(uncolouredSentence(290, 287)).toBe("3 of 290 tracts have no published value and are left uncoloured, not shown as zero.");
  });
});

describe("naming a tract the catalog no longer lists", () => {
  it("reads the tract number off its six-digit code", () => {
    expect(tractNameFromGeoId("state:48|county:453|tract:000101")).toBe("Census Tract 1.01");
    expect(tractNameFromGeoId("state:48|county:453|tract:001100")).toBe("Census Tract 11");
    expect(tractNameFromGeoId("state:48|county:453|tract:980000")).toBe("Census Tract 9800");
    expect(tractNameFromGeoId("state:48|county:453")).toBe("state:48|county:453");
  });
});
