// The uncoloured-is-not-zero rule, in the words every map legend uses.
//
// Tiles carry boundaries and no values (`/catalog/tile-layers`,
// `uncoloured_rule`). A map colours a geography only by joining a published
// value on `geo_id`, so a geography with no published value has no colour,
// and the legend says how many were left that way rather than letting a
// pale or empty shape read as zero. One sentence for tracts, places, metros
// and counties, so the rule is stated the same way on every map.

/** How a legend names the geographies it counts. */
export interface GeographyNoun {
  one: string;
  many: string;
}

export const GEOGRAPHY_NOUNS = Object.freeze({
  TRACT: { one: "tract", many: "tracts" },
  PLACE: { one: "city or town", many: "cities and towns" },
  METRO: { one: "metro area", many: "metro areas" },
  COUNTY: { one: "county", many: "counties" },
  STATE: { one: "state", many: "states" },
} satisfies Record<string, GeographyNoun>);

/** The sentence a legend states about the geographies a map left uncoloured. */
export function uncolouredSentence(noun: GeographyNoun, total: number, coloured: number): string {
  const missing = Math.max(total - coloured, 0);
  if (!missing) return `All ${total} ${noun.many} have a published value.`;
  return `${missing} of ${total} ${noun.many} have no published value and are left uncoloured, not shown as zero.`;
}
