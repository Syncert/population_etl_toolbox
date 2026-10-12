"use client";

// Where the numbers come from (find-your-place-home): one card per published
// source from the freshness resource, a refresh timeline, and the rules the
// site follows, written for a reader rather than a data steward. A source the
// freshness resource does not report says so; it is never shown as fresh.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { ExternalLink } from "lucide-react";
import StatusPill from "./StatusPill";
import { apiErrorMessage, getFreshness, getSources } from "../lib/api/client";
import type { SourceFreshness, SourceSummary } from "../lib/api/types";
import { formatDate, formatNumber } from "../lib/format";
import { SITE_RULES, sourceCards } from "../lib/placeDirectory";

const GRAIN_WORDS: Record<string, string> = {
  NATIONAL: "nation",
  STATE: "states",
  COUNTY: "counties",
  PLACE: "places",
  AGENCY: "law enforcement agencies",
};

export default function PublicDataPage() {
  const [sources, setSources] = useState<SourceSummary[]>([]);
  const [freshness, setFreshness] = useState<SourceFreshness[]>([]);
  const [status, setStatus] = useState({ state: "loading", message: "reading the sources" });

  useEffect(() => {
    const controller = new AbortController();
    Promise.all([getSources({ signal: controller.signal }), getFreshness({ signal: controller.signal })])
      .then(([sourceItems, freshnessPayload]) => {
        if (controller.signal.aborted) return;
        setSources(sourceItems);
        setFreshness(freshnessPayload.items || []);
        setStatus({ state: "ok", message: `${sourceItems.length} sources published` });
      })
      .catch((error) => {
        if (!controller.signal.aborted) setStatus({ state: "bad", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, []);

  const cards = useMemo(() => sourceCards(sources, freshness), [sources, freshness]);
  const timeline = useMemo(
    () => cards.filter((card) => card.lastRefresh).sort((left, right) => String(right.lastRefresh).localeCompare(String(left.lastRefresh))),
    [cards],
  );

  return (
    <main className="page-shell compact-page public-data-page" data-testid="public-data">
      <header className="page-heading">
        <div className="section-kicker">Where the numbers come from</div>
        <h1>The sources behind every number</h1>
        <p>
          Every value on this site was published by a public statistical program and is shown with its
          period and source. This page says when each source was last read, what it covers, and the rules
          the site follows.
        </p>
      </header>
      <section className="status-row" role="status">
        <StatusPill state={status.state} label="Sources" message={status.message} testId="public-data-status" />
      </section>

      <section aria-labelledby="sources-heading">
        <h2 id="sources-heading">Sources</h2>
        <div className="source-cards">
          {cards.map((card) => (
            <article key={card.sourceCode} className="source-card" data-testid={`source-card-${card.sourceCode}`} data-reported={card.reported ? "true" : "false"}>
              <h3>{card.name}</h3>
              <dl>
                <div><dt>Last refreshed here</dt><dd>{card.reported ? (card.lastRefresh ? formatDate(card.lastRefresh) : "not reported") : "not reported"}</dd></div>
                <div><dt>Newest publication</dt><dd>{card.reported && card.newestPublication ? formatDate(card.newestPublication) : "not reported"}</dd></div>
                <div><dt>Covers</dt><dd>{card.grains.length ? card.grains.map((grain) => GRAIN_WORDS[grain] || grain.toLowerCase()).join(", ") : "not reported"}</dd></div>
                <div><dt>Measures</dt><dd>{card.metricCount === null ? "not reported" : `${formatNumber(card.metricCount)}${card.staleCount ? `, ${formatNumber(card.staleCount)} awaiting refresh` : ""}`}</dd></div>
              </dl>
              {card.referenceUrl ? (
                <a className="text-link" href={card.referenceUrl} rel="noopener noreferrer" target="_blank">
                  The publisher&apos;s own documentation <ExternalLink size={13} aria-hidden="true" />
                </a>
              ) : null}
            </article>
          ))}
        </div>
      </section>

      <section aria-labelledby="timeline-heading">
        <h2 id="timeline-heading">Refresh timeline</h2>
        {timeline.length ? (
          <ol className="refresh-timeline" data-testid="refresh-timeline">
            {timeline.map((card) => (
              <li key={card.sourceCode}><strong>{formatDate(card.lastRefresh)}</strong> · {card.name}</li>
            ))}
          </ol>
        ) : (
          <p className="subtle">No refresh has been reported.</p>
        )}
        <p className="subtle">
          A source&apos;s revisions are kept rather than overwritten: the explorer&apos;s as-released view shows
          what each release published. This page reports the newest publication per source; which
          periods a release revised is not yet published here.
        </p>
      </section>

      <section aria-labelledby="rules-heading" id="rules">
        <h2 id="rules-heading">The rules every number follows</h2>
        <ol className="site-rules" data-testid="site-rules">
          {SITE_RULES.map((rule) => (
            <li key={rule.id} id={`rule-${rule.id}`}><strong>{rule.rule}</strong> {rule.detail}</li>
          ))}
        </ol>
      </section>

      <p>
        <Link className="text-link" href="/quality">The full data-quality explorer</Link>
      </p>
    </main>
  );
}
