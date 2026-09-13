import { describe, expect, test } from "vitest";

// Covers: WEB-064 — the explorer's geography picker offers the grain that was
// asked for. The grain selector publishes the whole declared vocabulary
// (WEB-038); the picker answered two grains and fell through to a third, so
// at PLACE — Census PEP's own grain — it offered states as the places, and at
// AGENCY it did the same. Picking one sent a geo_id that cannot exist at the
// selected grain, and the control could not even hold the choice.

import {
  GEO_GRAIN_ORDER,
  GRAINS_WITHIN_A_STATE,
  geographyName,
  geographyPickerState,
} from "../../../apps/web/lib/geographyPicker";
import { GEO_LEVELS } from "../../../apps/web/lib/urlState";

const STATE_ROW = {
  geo_id: "state:06",
  geo_level: "STATE",
  state_fips: "06",
  state_name: "California",
};
const COUNTY_ROW = {
  geo_id: "state:06|county:037",
  geo_level: "COUNTY",
  state_fips: "06",
  state_name: "California",
  county_name: "Los Angeles County",
};
// A place row as `/catalog/geographies` publishes it: its own name, no
// county_fips, and the state it sits in.
const PLACE_ROW = {
  geo_id: "state:06|place:44000",
  geo_level: "PLACE",
  state_fips: "06",
  state_name: "California",
  place_fips: "44000",
  place_name: "Los Angeles city",
};

describe("the grain vocabulary is spelled once", () => {
  test("the picker's order is the URL state's declared order", () => {
    // Two copies of a five-word vocabulary is how the picker came to offer
    // states as places.
    expect([...GEO_GRAIN_ORDER]).toEqual([...GEO_LEVELS]);
  });

  test("the grains chosen after a state are the ones inside a state", () => {
    expect([...GRAINS_WITHIN_A_STATE]).toEqual(["COUNTY", "PLACE"]);
    for (const grain of GRAINS_WITHIN_A_STATE) {
      expect(GEO_LEVELS).toContain(grain);
    }
  });
});

describe("a geography is named by its own grain", () => {
  test("each grain reads the field that attributes it", () => {
    expect(geographyName(STATE_ROW, "STATE")).toBe("California");
    expect(geographyName(COUNTY_ROW, "COUNTY")).toBe("Los Angeles County");
    // The field this application had never named, which is why a place had
    // no label to show.
    expect(geographyName(PLACE_ROW, "PLACE")).toBe("Los Angeles city");
  });

  test("a row publishing no name for its grain falls back to its identity", () => {
    // Never borrowing another grain's name: a place row carries a
    // `state_name` too, and "California" is not the name of a place.
    expect(geographyName({ ...PLACE_ROW, place_name: "" }, "PLACE")).toBe(
      "state:06|place:44000",
    );
    expect(geographyName({ ...PLACE_ROW, place_name: "   " }, "PLACE")).toBe(
      "state:06|place:44000",
    );
    expect(geographyName(null, "PLACE")).toBe("");
  });
});

describe("the picker answers for the selected grain", () => {
  test("a state grain offers states", () => {
    const picker = geographyPickerState("STATE", { geographies: [STATE_ROW] });
    expect(picker.label).toBe("State");
    expect(picker.placeholder).toBe("All states");
    expect(picker.disabled).toBe(false);
    expect(picker.options).toEqual([{ geoId: "state:06", name: "California" }]);
  });

  test("a county grain waits for a state, then offers that state's counties", () => {
    const waiting = geographyPickerState("COUNTY", { geographies: [COUNTY_ROW] });
    expect(waiting.placeholder).toBe("Select a state first");
    expect(waiting.disabled).toBe(true);
    expect(waiting.options).toEqual([]);

    const chosen = geographyPickerState("COUNTY", {
      geographies: [COUNTY_ROW],
      stateSelected: true,
    });
    expect(chosen.label).toBe("County");
    expect(chosen.placeholder).toBe("All counties");
    expect(chosen.options).toEqual([
      { geoId: "state:06|county:037", name: "Los Angeles County" },
    ]);
  });

  test("a place grain offers places, bounded by a state, and never states", () => {
    const waiting = geographyPickerState("PLACE", { geographies: [PLACE_ROW] });
    expect(waiting.placeholder).toBe("Select a state first");
    expect(waiting.disabled).toBe(true);

    const chosen = geographyPickerState("PLACE", {
      geographies: [PLACE_ROW],
      stateSelected: true,
    });
    expect(chosen.label).toBe("Place");
    expect(chosen.placeholder).toBe("All places");
    expect(chosen.options).toEqual([
      { geoId: "state:06|place:44000", name: "Los Angeles city" },
    ]);
  });

  test("a national view has no geography to choose", () => {
    const picker = geographyPickerState("NATIONAL", { geographies: [STATE_ROW] });
    expect(picker.placeholder).toBe("Not applicable for national view");
    expect(picker.disabled).toBe(true);
    expect(picker.options).toEqual([]);
  });

  test("a grain the projection publishes nothing for says so", () => {
    // The geography dimension's grains come from `dim_geo_current.geo_level`
    // — us, state, county, place — so it carries no agency identity. The
    // answer is a statement about the projection, not a list of states.
    const picker = geographyPickerState("AGENCY", { geographies: [] });
    expect(picker.placeholder).toBe("No agencies are published to choose from");
    expect(picker.disabled).toBe(true);
    expect(picker.options).toEqual([]);
  });

  test("nothing arrived yet is not nothing published", () => {
    // Only one of those two is a fact about the warehouse, and only it is
    // said. A picker that reports "none published" while its read is still
    // in flight is asserting something it has not been told.
    const loading = geographyPickerState("AGENCY", { geographies: [], read: false });
    expect(loading.placeholder).toBe("Loading agencies…");
    expect(loading.disabled).toBe(true);

    const loadingStates = geographyPickerState("STATE", {
      geographies: [],
      read: false,
    });
    expect(loadingStates.placeholder).toBe("Loading states…");
  });

  test("a row with no identity is not an option", () => {
    // An option whose value is empty is the placeholder again, and picking
    // it would read as clearing the selection.
    const picker = geographyPickerState("STATE", {
      geographies: [{ state_name: "Nowhere" }, STATE_ROW],
    });
    expect(picker.options).toEqual([{ geoId: "state:06", name: "California" }]);
  });

  test("an unknown grain is answered in its own terms, not another's", () => {
    // The vocabulary can grow. A grain with no label here is still never
    // answered with a list belonging to a different grain.
    const picker = geographyPickerState("TRACT", { geographies: [] });
    expect(picker.label).toBe("TRACT");
    expect(picker.placeholder).toBe("No tract geographies are published to choose from");
    expect(picker.options).toEqual([]);
  });

  test("every declared grain gets an answer of its own", () => {
    for (const grain of GEO_LEVELS) {
      const picker = geographyPickerState(grain, {
        geographies: [STATE_ROW],
        stateSelected: true,
      });
      expect(picker.placeholder, grain).toBeTruthy();
      // A grain's options are labelled by that grain's own field, so a
      // state row offered at another grain shows its identity rather than
      // "California" standing in for a place or an agency.
      if (grain !== "STATE" && picker.options.length > 0) {
        expect(picker.options[0].name, grain).toBe("state:06");
      }
    }
  });
});
