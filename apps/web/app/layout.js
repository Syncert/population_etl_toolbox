import "./globals.css";
import ClientReporters from "../components/ClientReporters";
import SiteHeader from "../components/SiteHeader";

// Every route renders per request. The Content-Security-Policy carries a
// per-request nonce (see middleware.ts), and a nonce cannot be baked into a
// prerendered page: a static build emits script tags with no nonce against a
// policy that demands one, and the browser then blocks every one of them --
// a page that ships blank. Under `next dev` that failure is invisible, because
// dev renders per request anyway; `scripts/check-csp-nonce.mjs` guards the
// production build for it.
export const dynamic = "force-dynamic";

export const metadata = {
  title: {
    default: "Economic Data Studio",
    template: "%s | Economic Data Studio",
  },
  // Not an enumeration of sources: this named three while the API served
  // seven, and a document description cannot be derived from a request
  // (WEB-080).
  description:
    "Traceable public economic, health, agricultural and population analytics, "
    + "every value tied back to the agency that published it.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        {/* The first focusable element on every page. Ten navigation links
            stand between the top of the document and the analysis on it, and
            a keyboard reader met all ten on every route. `.sr-only` already
            existed in the stylesheet; nothing used it. */}
        <a className="skip-link" href="#main-content">
          Skip to main content
        </a>
        <SiteHeader />
        {/* The skip link's target lives here rather than on each route's own
            `<main>`: there are fifteen of those across thirteen files, and
            "someone adds a route and forgets the id" is the failure this
            whole gate exists to prevent. `tabIndex={-1}` makes it focusable
            by the link without putting it in the tab order. */}
        <div id="main-content" tabIndex={-1}>
          {children}
        </div>
        {/* Registered once, for every route. It renders nothing; what it
            does is give the browser a way to say what went wrong, which
            until now it had none of (WEB-114). */}
        <ClientReporters />
        <footer className="site-footer">
          <span>Economic Data Studio</span>
          <span>Public data, source-visible by design.</span>
        </footer>
      </body>
    </html>
  );
}
