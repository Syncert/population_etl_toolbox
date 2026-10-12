import Link from "next/link";
import { notFound } from "next/navigation";

import ExplainerExample from "../../../components/ExplainerExample";
import { loadExplainer } from "../../../lib/explainerFiles";

export async function generateMetadata({ params }) {
  const explainer = loadExplainer((await params).slug);
  if (!explainer) notFound();
  return { title: explainer.title, description: explainer.summary };
}

function Blocks({ blocks }) {
  return blocks.map((block, index) =>
    block.kind === "list" ? (
      <ul key={index}>{block.items.map((item) => <li key={item}>{item}</li>)}</ul>
    ) : (
      <p key={index}>{block.text}</p>
    ),
  );
}

export default async function ExplainerPage({ params }) {
  const explainer = loadExplainer((await params).slug);
  if (!explainer) notFound();
  return (
    <main className="page-shell compact-page explainer-page" data-testid="explainer" data-slug={explainer.slug}>
      <header className="page-heading">
        <div className="section-kicker">Explainer</div>
        <h1>{explainer.title}</h1>
        <p>{explainer.summary}</p>
      </header>
      {explainer.sections.map((section) => (
        <section key={section.title} className="explainer-section" aria-labelledby={`section-${section.title}`}>
          <h2 id={`section-${section.title}`}>{section.title}</h2>
          <Blocks blocks={section.blocks} />
          {section.title === "Worked example" ? <ExplainerExample metricCode={explainer.exampleMetric} /> : null}
        </section>
      ))}
      <footer className="explainer-footer subtle">
        <p>
          Applies to {explainer.metricCodes.join(", ")} ({explainer.sources.join(", ")}). Reviewed{" "}
          {explainer.reviewed} · {explainer.reviewer}.
        </p>
        {explainer.definitions.length ? <p>Reviewed definitions: {explainer.definitions.join(", ")}.</p> : null}
        {explainer.videoUrl ? (
          <p><a className="text-link" href={explainer.videoUrl} rel="noopener noreferrer">Watch the video</a></p>
        ) : null}
        <p><Link className="text-link" href="/explain">All explainers</Link></p>
      </footer>
    </main>
  );
}
