// The single age-by-sex lines of ACS table B01001 (web-age-and-sex-structure).
//
// Each line is one sex in one of the Census Bureau's 23 unequal age groups
// ("Male: 20 years", "Male: 22 to 24 years"). On its own it is a sliver of a
// pyramid, and a county map of one sliver invites reading a one-year group
// beside a three-year one as if they were alike. The one-measure map no
// longer offers them (ADR-0013); they stay readable in the explorer, and the
// place pages draw them grouped into five-year bands as a pyramid. The table
// total (B01001_001) and the two sex totals are ordinary counts and still map.

const SINGLE_AGE_LINE = /^CENSUS_ACS:acs[15]:B01001_0(0[3-9]|1\d|2[0-5]|2[7-9]|[34]\d)$/;

export function isSingleAgeLine(metricCode: string): boolean {
  return SINGLE_AGE_LINE.test(metricCode);
}
