import Link from "next/link";
import { ArrowRight, Layers, ShieldCheck } from "lucide-react";
import type { UseCasePage } from "../lib/useCasePages";

export default function UseCaseIntro({ entry }: { entry: UseCasePage }) {
  return (
    <>
      <nav className="use-case-breadcrumb" aria-label="Breadcrumb">
        <Link href="/use-cases">Use cases</Link><span aria-hidden="true">/</span>
        <Link href={`/use-cases#${entry.group}`}>{entry.groupTitle}</Link><span aria-hidden="true">/</span>
        <span>Use case {String(entry.rank).padStart(2, "0")}</span>
      </nav>
      <header className="use-case-hero" data-testid="use-case-hero">
        <div>
          <div className="section-kicker">{entry.groupTitle} · {String(entry.rank).padStart(2, "0")} / 20</div>
          <h1>{entry.title}</h1>
          <p>{entry.question}</p>
          <div className="use-case-products"><Layers size={15} aria-hidden="true" />{entry.products}</div>
        </div>
        <aside className="use-case-audience"><span className="section-kicker">Built for</span><p>{entry.audience}</p><span>Provider facts. Inspectable context.</span></aside>
      </header>
      <section className="use-case-guardrail" aria-label="Interpretation guardrail" data-testid="template-limits">
        <ShieldCheck size={22} aria-hidden="true" /><div><strong>Read the evidence in context</strong><p>{entry.guardrail}</p></div>
      </section>
      <section className="use-case-steps" aria-label="Suggested workflow">
        {entry.steps.map((step, index) => <div key={step}><span>{String(index + 1).padStart(2, "0")}</span><p>{step}</p></div>)}
      </section>
      <div className="use-case-tool-row">
        <Link className="text-link" href={`/catalog?q=${encodeURIComponent(entry.catalogQuery)}`}>Find topic measures <ArrowRight size={14} /></Link>
        {entry.tool === "evidence" ? <>
          <Link className="button primary" href="/builder">Compose evidence packet</Link>
          <Link className="text-link" href="/saved">Open saved evidence</Link>
          <Link className="text-link" href="/articles">Preview story</Link>
        </> : <Link className="text-link" href="/quality">Check source quality <ArrowRight size={14} /></Link>}
      </div>
    </>
  );
}
