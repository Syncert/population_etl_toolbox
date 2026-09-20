// Which routes are published, and which belong to a reader alone.
//
// Stated once because `robots.js` and `sitemap.js` must not disagree: a
// sitemap naming a route `robots.txt` disallows is a contradiction a crawler
// resolves for itself.

/** Routes a crawler may index. */
export const PUBLIC_ROUTES: readonly string[] = [
  "/",
  "/catalog",
  "/explore",
  "/compare",
  "/workbench",
  "/profiles",
  "/quality",
  "/articles",
];

/**
 * Routes that are a reader's own workspace rather than published analysis.
 *
 * `/saved` lists an account's stored analyses; `/builder` holds a draft
 * evidence packet. Excluding them is a statement about the published surface,
 * not the control that protects them -- that is the API's token check.
 */
export const PRIVATE_ROUTES: readonly string[] = ["/saved", "/builder"];

/**
 * The origin the sitemap's absolute URLs are built from.
 *
 * A sitemap entry has to be absolute, and a deployment knows its own origin
 * where the application does not. `NEXT_PUBLIC_SITE_URL` names it; unset, this
 * falls back to localhost, which is wrong for a deployment and obviously so --
 * better than a plausible guess at someone's domain.
 */
export function siteOrigin(): string {
  const configured = process.env.NEXT_PUBLIC_SITE_URL || "";
  return configured.replace(/\/+$/, "") || "http://localhost:3000";
}
