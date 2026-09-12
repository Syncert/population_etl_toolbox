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
  description: "Traceable public economic and population analytics from Census, BLS, and FRED.",
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
