"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, BookOpen, Bookmark, Columns3, Database, FilePenLine, LineChart, MapPinned, ShieldCheck } from "lucide-react";

const navigation = [
  { href: "/", label: "Home" },
  { href: "/catalog", label: "Data Catalog", icon: Database },
  { href: "/explore", label: "Explore", icon: BarChart3 },
  { href: "/compare", label: "Compare", icon: Columns3 },
  // "Build" already means "compose a document" on this site — /builder is the
  // evidence packet composer — so the chart composer is the Workbench.
  { href: "/workbench", label: "Workbench", icon: LineChart },
  { href: "/profiles", label: "Profiles", icon: MapPinned },
  { href: "/quality", label: "Data quality", icon: ShieldCheck },
  { href: "/articles", label: "Articles", icon: BookOpen },
  { href: "/builder", label: "Builder", icon: FilePenLine },
  { href: "/saved", label: "Saved", icon: Bookmark },
];

export default function SiteHeader() {
  const pathname = usePathname();

  return (
    <header className="site-header">
      <Link className="wordmark" href="/" aria-label="Economic Data Studio home">
        <span className="wordmark-mark">EDS</span>
        <span>Economic Data Studio</span>
      </Link>
      <nav className="primary-nav" aria-label="Primary navigation">
        {navigation.map(({ href, label, icon: Icon }) => {
          const active = href === "/" ? pathname === href : pathname.startsWith(href);
          return (
            <Link
              className={active ? "nav-link active" : "nav-link"}
              href={href}
              key={href}
              // The active link was marked by a class alone, which says
              // nothing to anyone who is not looking at the colour.
              aria-current={active ? "page" : undefined}
            >
              {Icon ? <Icon aria-hidden="true" size={15} /> : null}
              <span>{label}</span>
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
