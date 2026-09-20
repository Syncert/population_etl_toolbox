import { PUBLIC_ROUTES, siteOrigin } from "../lib/siteMap";

// The public routes, and only those. A sitemap is a claim about what is worth
// indexing, so it names the same list `robots.js` allows rather than a second
// copy of it that could drift.
export default function sitemap() {
  const origin = siteOrigin();
  return PUBLIC_ROUTES.map((route) => ({
    url: `${origin}${route === "/" ? "" : route}`,
    changeFrequency: "daily",
  }));
}
