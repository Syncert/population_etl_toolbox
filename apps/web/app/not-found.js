import Link from "next/link";

// The site's own 404, in the site's own shell.
//
// Next serves a bare built-in page for an unmatched path unless the root
// segment provides this one, so a mistyped or stale link dropped the reader
// out of the application entirely -- no header, no navigation, nothing to
// click. It keeps `main` and one `h1` for the same reason the error boundary
// does.

export const metadata = {
  title: "Page not found",
};

export default function NotFound() {
  return (
    <main className="page-shell compact-page" data-testid="route-not-found">
      <div className="page-heading">
        <div className="section-kicker">Not found</div>
        <h1>There is no page at this address</h1>
        <p>
          The link may be stale, or the address mistyped. Nothing was lost;
          the catalog and the explorer are where they were.
        </p>
      </div>
      <div className="command-row">
        <Link className="button primary" href="/explore">
          Explore the data
        </Link>
        <Link className="button secondary" href="/catalog">
          Browse the catalog
        </Link>
      </div>
    </main>
  );
}
