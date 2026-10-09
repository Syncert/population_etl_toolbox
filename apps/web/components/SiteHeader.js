"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useRef } from "react";

import SignInControl from "./SignInControl";
import { BarChart3, BookOpen, Bookmark, ChevronDown, Columns3, Database, FilePenLine, Landmark, Layers, LineChart, MapPinned, ShieldCheck, Wrench } from "lucide-react";

// A resident's entries first (find-your-place-home); the analyst tools keep
// every route they had, one level down under "Tools".
const navigation = [
  // One page per place: the nation, each state, each county (place-pages).
  { href: "/us", label: "Find your place", icon: Landmark },
  { href: "/data", label: "Where the numbers come from", icon: ShieldCheck },
];

const tools = [
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

function isActive(href, pathname) {
  return href === "/" ? pathname === href : pathname.startsWith(href);
}

export default function SiteHeader({ useCaseGroups = [] }) {
  const pathname = usePathname();
  const dropdown = useRef(null);
  const toolsMenu = useRef(null);
  useEffect(() => {
    const dismiss = (event) => {
      for (const menu of [dropdown.current, toolsMenu.current]) {
        if (!menu?.open) continue;
        if (event.type === "keydown" && event.key === "Escape") {
          menu.open = false;
          menu.querySelector("summary")?.focus();
        } else if (event.type === "pointerdown" && !menu.contains(event.target)) {
          menu.open = false;
        }
      }
    };
    document.addEventListener("keydown", dismiss);
    document.addEventListener("pointerdown", dismiss);
    return () => {
      document.removeEventListener("keydown", dismiss);
      document.removeEventListener("pointerdown", dismiss);
    };
  }, []);

  return (
    <header className="site-header">
      <Link className="wordmark" href="/" aria-label="Economic Data Studio home">
        <span className="wordmark-mark">EDS</span>
        <span>Economic Data Studio</span>
      </Link>
      <nav className="primary-nav" aria-label="Primary navigation">
        <details className="use-case-menu" ref={dropdown} data-testid="use-case-menu">
          <summary className={`nav-link${pathname.startsWith("/use-cases") ? " active" : ""}`}><Layers size={15} aria-hidden="true" />Use cases<ChevronDown size={13} aria-hidden="true" /></summary>
          <div className="use-case-dropdown">
            <div className="use-case-dropdown-heading"><strong>Start with a question</strong><Link href="/use-cases" onClick={() => { dropdown.current.open = false; }}>All 20 use cases →</Link></div>
            <div className="use-case-menu-groups">
              {useCaseGroups.map((group) => <details key={group.id} className="use-case-submenu">
                <summary>{group.title}<span>{group.pages.length}</span><ChevronDown size={14} aria-hidden="true" /></summary>
                <ul>{group.pages.map((entry) => <li key={entry.id}><Link href={entry.href} aria-current={pathname === entry.href ? "page" : undefined} onClick={() => { dropdown.current.open = false; }}><span>{String(entry.rank).padStart(2, "0")}</span>{entry.title}</Link></li>)}</ul>
              </details>)}
            </div>
          </div>
        </details>
        {navigation.map(({ href, label, icon: Icon }) => {
          const active = isActive(href, pathname);
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
        <details className="use-case-menu tools-menu" ref={toolsMenu} data-testid="tools-menu">
          <summary className={`nav-link${tools.some(({ href }) => isActive(href, pathname)) ? " active" : ""}`}><Wrench size={15} aria-hidden="true" />Tools<ChevronDown size={13} aria-hidden="true" /></summary>
          <ul className="tools-dropdown">
            {tools.map(({ href, label, icon: Icon }) => {
              const active = isActive(href, pathname);
              return (
                <li key={href}>
                  <Link
                    className={active ? "nav-link active" : "nav-link"}
                    href={href}
                    aria-current={active ? "page" : undefined}
                    onClick={() => { toolsMenu.current.open = false; }}
                  >
                    {Icon ? <Icon aria-hidden="true" size={15} /> : null}
                    <span>{label}</span>
                  </Link>
                </li>
              );
            })}
          </ul>
        </details>
      </nav>
      {/* Last in the header and outside the nav: signing in is not a place on
          this site, and putting it in the primary navigation would make it
          one. */}
      <SignInControl />
    </header>
  );
}
