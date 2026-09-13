import "./globals.css";
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
        <SiteHeader />
        {children}
        <footer className="site-footer">
          <span>Economic Data Studio</span>
          <span>Public data, source-visible by design.</span>
        </footer>
      </body>
    </html>
  );
}
