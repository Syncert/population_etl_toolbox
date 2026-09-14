// What the explorer's geography picker offers, and what it says when it
// offers nothing.
//
// The grain selector beside it publishes the whole declared vocabulary
// (WEB-038). The picker answered two grains and fell through to a third, so
// at PLACE -- Census PEP's own grain -- it offered states as the places to
// choose from, and at AGENCY it did the same. Deciding it here, as one
// function over the grain and the lists, is what keeps a grain the
// application offers from being answered with another grain's geographies
// (WEB-064).

import type { GeographySummary } from "./api/types";
import { GEO_LEVELS } from "./urlState";

/** The declared grain vocabulary, broadest first, as the URL state spells it. */
export const GEO_GRAIN_ORDER: readonly string[] = GEO_LEVELS;

/** One grain's name in the reader's terms, singular and plural. */
export const GEO_GRAIN_LABELS: Record<string, { one: string; many: string }> = {
  NATIONAL: { one: "National", many: "national geographies" },
  STATE: { one: "State", many: "states" },
  COUNTY: { one: "County", many: "counties" },
  PLACE: { one: "Place", many: "places" },
  AGENCY: { one: "Agency", many: "agencies" },
};

/**
 * Grains that sit inside a state, and are therefore chosen after one.
 *
 * This is what bounds the read: the projection carries some 32k places, and
 * a `<select>` is not the place to load them all. COUNTY already worked this
 * way; PLACE does for the same reason. NATIONAL and STATE are not inside a
 * state, and AGENCY is not projected into the geography dimension at all --
 * its grains come from `dim_geo_current.geo_level`, which is
 * `us`/`state`/`county`/`place`.
 */
export const GRAINS_WITHIN_A_STATE: readonly string[] = ["COUNTY", "PLACE"];

/** One offered geography: the identity to send, and the name to show. */
export interface GeographyOption {
  geoId: string;
  name: string;
}

export interface GeographyPickerState {
  /** The control's label, naming the grain actually being chosen. */
  label: string;
  options: GeographyOption[];
  /** What the empty option says: never another grain's invitation. */
  placeholder: string;
  disabled: boolean;
}

function grainLabels(geoLevel: string): { one: string; many: string } {
  return (
    GEO_GRAIN_LABELS[geoLevel] || {
      one: geoLevel || "Geography",
      many: `${(geoLevel || "geography").toLowerCase()} geographies`,
    }
  );
}

/**
 * The name a row publishes for its own grain.
 *
 * Read from the field that grain is attributed by -- `place_name` for a
 * place, which the served contract publishes and this application had never
 * named -- and never from another grain's field. A row publishing no name
 * for its grain falls back to its identity rather than borrowing one.
 */
export function geographyName(
  row: GeographySummary | null | undefined,
  geoLevel: string,
): string {
  if (!row) {
    return "";
  }
  const byGrain: Record<string, unknown> = {
    STATE: row.state_name,
    COUNTY: row.county_name,
    PLACE: row.place_name,
  };
  const published = byGrain[String(geoLevel || "").toUpperCase()];
  const name = typeof published === "string" ? published.trim() : "";
  return name || String(row.geo_id || "");
}

/**
 * The picker for one grain.
 *
 * `read` says whether the projection has answered for this grain yet, which
 * is what separates "nothing is published at this grain" from "nothing has
 * arrived". Only the first is a statement about the warehouse, and only it
 * is said.
 */
export function geographyPickerState(
  geoLevel: string,
  {
    geographies = [],
    stateSelected = false,
    read = true,
  }: {
    geographies?: GeographySummary[];
    stateSelected?: boolean;
    read?: boolean;
  } = {},
): GeographyPickerState {
  const grain = String(geoLevel || "").toUpperCase();
  const labels = grainLabels(grain);
  const empty = { label: labels.one, options: [], disabled: true };

  if (grain === "NATIONAL") {
    return { ...empty, placeholder: "Not applicable for national view" };
  }
  if (GRAINS_WITHIN_A_STATE.includes(grain) && !stateSelected) {
    return { ...empty, placeholder: "Select a state first" };
  }
  if (!read) {
    return { ...empty, placeholder: `Loading ${labels.many}…` };
  }

  const options = geographies
    .filter((row) => row && row.geo_id)
    .map((row) => ({ geoId: String(row.geo_id), name: geographyName(row, grain) }));

  if (options.length === 0) {
    // Derived from the answer, not from a list of which grains have
    // geographies: the projection is refreshed on its own schedule, so
    // "publishes none" is a fact to read rather than one to assume.
    return {
      ...empty,
      placeholder: `No ${labels.many} are published to choose from`,
    };
  }
  return {
    label: labels.one,
    options,
    placeholder: `All ${labels.many}`,
    disabled: false,
  };
}
