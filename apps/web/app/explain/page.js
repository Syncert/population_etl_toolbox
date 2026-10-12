import Link from "next/link";

import { loadExplainers } from "../../lib/explainerFiles";
import { STATIC_ROUTE_TITLES } from "../../lib/routeTitles";

export const metadata = {
  title: STATIC_ROUTE_TITLES["/explain"],
  description: "Plain-language answers to the caveats the published data carries.",
};

export default function ExplainersPage() {
  const explainers = loadExplainers();
  return (
    <main className="page-shell compact-page explainer-page" data-testid="explainer-list">
      <header className="page-heading">
        <div className="section-kicker">Explainers</div>
        <h1>What the numbers do and do not say</h1>
        <p>
          Each caveat the data carries, answered once in plain language and linked from every chart that
          carries it.
        </p>
      </header>
      <ul className="explainer-index">
        {explainers.map((explainer) => (
          <li key={explainer.slug}>
            <Link href={`/explain/${explainer.slug}`} data-testid="explainer-link">{explainer.title}</Link>
            <p className="subtle">{explainer.summary}</p>
          </li>
        ))}
      </ul>
    </main>
  );
}
