import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Population Geospatial Analytics",
  description: "First-pass geospatial analytics shell",
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
