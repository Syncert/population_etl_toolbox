"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { BarChart3, BookOpen, Database, FilePenLine, MapPinned } from "lucide-react";

const navigation = [
  { href: "/", label: "Home" },
  { href: "/catalog", label: "Data Catalog", icon: Database },
  { href: "/explore", label: "Explore", icon: BarChart3 },
  { href: "/profiles", label: "Profiles", icon: MapPinned },
  { href: "/articles", label: "Articles", icon: BookOpen },
  { href: "/builder", label: "Builder", icon: FilePenLine },
];

export default function SiteHeader() {
  const pathname = usePathname();
  const isSourceDashboard = ["/bls", "/census", "/fred"].some(
    (path) => pathname === path || pathname.startsWith(`${path}/`),
  );

  if (isSourceDashboard) {
    return null;
  }

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
            <Link className={active ? "nav-link active" : "nav-link"} href={href} key={href}>
              {Icon ? <Icon aria-hidden="true" size={15} /> : null}
              <span>{label}</span>
            </Link>
          );
        })}
      </nav>
    </header>
  );
}
