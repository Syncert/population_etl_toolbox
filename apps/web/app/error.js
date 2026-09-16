"use client";

import Link from "next/link";

// A route segment's error boundary.
//
// Without one, a throw anywhere under `app/` falls to Next's own generic
// error page: the site header goes, the analysis on screen goes, and the
// reader is shown a page that does not belong to this application and offers
// no way back to what they were doing. Two of this application's own writers
// could produce that throw -- `localStorage.setItem` raises a
// `QuotaExceededError` or a `SecurityError` rather than returning null -- and
// those are guarded now, but a boundary is not a patch for one known throw.
// It is where any of them lands.
//
// The failure page keeps `main` and one `h1`, because the accessibility audit
// asserts one of each on every route and a failure page is a route a reader
// can be on. It is a client component: an error boundary has to be.

export default function RouteError({ error, reset }) {
  return (
    <main className="page-shell compact-page" data-testid="route-error">
      <div className="page-heading">
        <div className="section-kicker">Something went wrong</div>
        <h1>This page could not be shown</h1>
        <p>
          The rest of the site is unaffected. Try again — the failure may have
          been momentary — or go back to the selection you came from.
          {error?.digest ? (
            <>
              {" "}
              Quote <code>{error.digest}</code> if you report it.
            </>
          ) : null}
        </p>
      </div>
      <div className="command-row">
        {/* `reset` re-renders this segment. The address bar is untouched by a
            boundary, so whatever state the route carries in the URL is still
            there and a successful retry reopens the same view. */}
        <button type="button" className="button primary" onClick={() => reset()}>
          Try this page again
        </button>
        <Link className="button secondary" href="/explore">
          Back to the selection
        </Link>
      </div>
    </main>
  );
}
