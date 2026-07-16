import "./globals.css";
import SiteHeader from "../components/SiteHeader";

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
