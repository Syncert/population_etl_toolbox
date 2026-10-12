// The example place the home page offers before briefings exist
// (find-your-place-home): one county with its state and the nation, so a
// first-time reader sees all three levels. Configuration, resolved through
// the catalog; a deployment that does not publish it simply shows fewer links.
export const FEATURED_PLACE = {
  countyGeoId: "state:55|county:025",
  stateGeoId: "state:55",
} as const;
