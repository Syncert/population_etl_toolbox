// The four monthly climate normals the place page's Climate averages section
// reads (place-climate-normals), in their own module so the page's one
// catalog read can name them without carrying the section's code: the rules
// are `lib/climateNormals.ts`, loaded with the lazy section.
export const NORMALS_CODES: readonly string[] = [
  "NOAA_NORMALS:monthly_mean_temperature",
  "NOAA_NORMALS:monthly_mean_maximum_temperature",
  "NOAA_NORMALS:monthly_mean_minimum_temperature",
  "NOAA_NORMALS:monthly_precipitation",
];
