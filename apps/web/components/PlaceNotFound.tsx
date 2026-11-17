"use client";

// What a place address that names nothing shows: why, and a search over the
// places that could have been meant. Its own chunk: a page that resolves
// never loads it.

import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { useState } from "react";

export default function PlaceNotFound({ message, candidates }: { message: string; candidates: { name: string; href: string }[] }) {
  const [search, setSearch] = useState("");
  const needle = search.trim().toLowerCase();
  const matches = needle ? candidates.filter((entry) => entry.name.toLowerCase().includes(needle)).slice(0, 20) : [];
  return (
    <main className="page-shell compact-page" data-testid="place-not-found">
      <div className="page-heading">
        <div className="section-kicker">Not found</div>
        <h1>There is no place at this address</h1>
        <p>{message}</p>
      </div>
      <label className="place-search">
        Search places
        <input type="search" value={search} onChange={(event) => setSearch(event.target.value)} data-testid="place-search" />
      </label>
      {needle ? (
        <ul className="place-index" data-testid="place-search-results">
          {matches.map((entry) => <li key={entry.href}><Link href={entry.href}>{entry.name}</Link></li>)}
          {!matches.length ? <li>No published place matches “{search}”.</li> : null}
        </ul>
      ) : null}
      <p><Link className="text-link" href="/us">All states <ArrowRight size={13} /></Link></p>
    </main>
  );
}
