import { PRIVATE_ROUTES, PUBLIC_ROUTES, siteOrigin } from "../lib/siteMap";

// What a crawler may index.
//
// The private routes are excluded because they are a reader's own workspace,
// not published analysis: `/saved` lists an account's stored analyses and
// `/builder` holds a draft packet. Neither is reachable without a token, so
// this is not the control that protects them -- it is the statement that they
// are not part of the published surface.
export default function robots() {
  return {
    rules: [{ userAgent: "*", allow: PUBLIC_ROUTES, disallow: PRIVATE_ROUTES }],
    sitemap: `${siteOrigin()}/sitemap.xml`,
  };
}
