import "./globals.css";

export const metadata = {
  title: "Population ETL Dashboard",
  description: "Local dashboard for health, metrics, observations, and map preview.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
