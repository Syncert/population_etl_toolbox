import Link from "next/link";

import { useCasePages } from "../../lib/useCasePages";

export const dynamic = "force-dynamic";
export const metadata = { title: "Analytics use cases" };

export default function UseCasesPage() {
  return (
    <main className="page-shell">
      <header className="page-heading">
        <div className="section-kicker">Public data, ready to explore</div>
        <h1>Analytics use cases</h1>
        <p>Choose a question, then a place. Each page reads published measures with their own source, period, and limits.</p>
      </header>
      <section className="path-grid" aria-label="Available use cases">
        {useCasePages.map((entry) => (
          <Link href={entry.href} key={entry.id} data-testid="use-case-link">
            <strong>{entry.title}</strong>
            <span>{entry.summary}</span>
          </Link>
        ))}
      </section>
      <p className="subtle">More topics are being reviewed against the published catalog. <Link href="/profiles">Browse all profile templates</Link> or <Link href="/catalog">search the catalog</Link>.</p>
    </main>
  );
}
