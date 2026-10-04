"use client";

import { useState } from "react";
import Link from "next/link";
import { ArrowRight, Search } from "lucide-react";
import type { UseCasePage } from "../lib/useCasePages";

type DirectoryGroup = { id: string; title: string; description: string; pages: Pick<UseCasePage, "id" | "href" | "title" | "rank" | "question" | "audience" | "products" | "group">[] };

export default function UseCaseDirectory({ useCaseGroups }: { useCaseGroups: DirectoryGroup[] }) {
  const [search, setSearch] = useState("");
  const query = search.trim().toLowerCase();
  const groups = useCaseGroups.map((group) => ({ ...group, pages: group.pages.filter((entry) => `${entry.title} ${entry.audience} ${entry.products} ${entry.question}`.toLowerCase().includes(query)) })).filter((group) => group.pages.length);
  const count = groups.reduce((total, group) => total + group.pages.length, 0);
  return (
    <main className="page-shell use-case-directory">
      <header className="use-case-directory-hero">
        <div className="section-kicker">The public-data field guide</div>
        <h1>Better questions.<br /><span>Clearer evidence.</span></h1>
        <p>Twenty ways to turn public data into useful context. Start with a community question, follow the published measures, and keep every source in view.</p>
        <div className="use-case-directory-facts"><span><strong>20</strong> use cases</span><span><strong>6</strong> topic collections</span><span><strong>7</strong> packaged products</span></div>
      </header>
      <div className="use-case-directory-controls">
        <label className="use-case-search"><Search size={18} aria-hidden="true" /><span className="sr-only">Search use cases</span><input type="search" value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Search a question, audience, or source…" /></label>
        <span role="status">{count} of {useCaseGroups.reduce((total, group) => total + group.pages.length, 0)} use cases</span>
      </div>
      <nav className="use-case-collections" aria-label="Use case collections">{useCaseGroups.map((group) => <a href={`#${group.id}`} key={group.id}>{group.title}<span>{group.pages.length}</span></a>)}</nav>
      {groups.length ? groups.map((group) => <section className="use-case-collection" id={group.id} key={group.id} aria-labelledby={`collection-${group.id}`}>
        <header><div><h2 id={`collection-${group.id}`}>{group.title}</h2><p>{group.description}</p></div><span>{group.pages.length} pathways</span></header>
        <div className="use-case-card-grid">{group.pages.map((entry) => <Link href={entry.href} key={entry.id} className={`use-case-card use-case-${entry.group}`} data-testid="use-case-link">
          <div className="use-case-card-top"><span className="use-case-number">{String(entry.rank).padStart(2, "0")}</span><span>{entry.products}</span><ArrowRight size={18} aria-hidden="true" /></div>
          <h3>{entry.title}</h3><p>{entry.question}</p><div className="use-case-card-audience">For {entry.audience}</div>
        </Link>)}</div>
      </section>) : <p className="empty-state">No use cases match “{search}”. Try a topic such as housing, health, or workforce.</p>}
      <aside className="use-case-directory-note"><strong>Source transparency comes first.</strong><p>Each page resolves measures against this deployment’s published catalog. Availability varies by place and source. Missing and suppressed values stay visible, and cross-source patterns describe association.</p><Link className="text-link" href="/catalog">Explore the full catalog <ArrowRight size={14} /></Link></aside>
    </main>
  );
}
