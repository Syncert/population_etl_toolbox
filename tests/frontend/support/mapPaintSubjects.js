/** Drawable grains the deployment's active catalog actually publishes. */
export function deploymentPaintGrains(metrics, drawable) {
  const published = new Set();
  for (const metric of metrics) {
    for (const grain of metric.valid_geo_grains || []) {
      published.add(String(grain).toUpperCase());
    }
  }
  return drawable.filter((grain) => published.has(grain));
}

/** Reviewed sources that declare a spatial map, excluding national-only data. */
export function reviewedPaintSources(advertisedGrains, drawable) {
  return Object.entries(advertisedGrains)
    .filter(([, grains]) => grains.some((grain) => drawable.includes(grain)))
    .map(([sourceCode]) => sourceCode);
}
