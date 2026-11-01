"use client";

// "What the record shows" (D-9, ADR-0011): four fixed questions answered on
// every place page, each with a chart, sentences built from the published
// numbers, who reported (for crime), the sources and caveats, and a link to
// the rows it was made from. The wording is `lib/checkTheRecord.ts`'s.
//
// It reads what the chapters already read. A county page adds one read: its
// violent crime rate history computed from agency reports (ADR-0012), asked
// only once the county is known, so no request names a placeholder.

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import PlaceTrend from "./PlaceTrend";
import { apiErrorMessage, buildApiPath } from "../lib/api/client";
import { agencyRateFor, getDerivedObservations } from "../lib/derivedViews";
import type { DerivedRatioPairing } from "../lib/derivedViews";
import type { ObservationRow } from "../lib/explorerViewModel";
import type { LevelAnswer, LevelPlace, PlaceLevel, ResolvedChapter, ResolvedPlaceMeasure } from "../lib/placeChapters";
import { recordAnswers } from "../lib/checkTheRecord";
import type { CountyCrimeHistory, RecordAnswer } from "../lib/checkTheRecord";

/** The FBI rate the county agency rate stands beside (ADR-0012). */
const VIOLENT_RATE = "FBI_UCR:summarized_violent_crime:V:offense:rate";
/** Eleven years of months, with room: the record compares at most ten years. */
const CRIME_HISTORY_LIMIT = 300;

export default function CheckTheRecord({
  level,
  levels,
  shown,
  histories,
  answers,
  pairings,
  countyPage,
  exploreLink,
}: {
  level: PlaceLevel;
  levels: LevelPlace[];
  shown: ResolvedChapter[];
  histories: Map<string, ObservationRow[]>;
  answers: Map<string, LevelAnswer>;
  pairings: DerivedRatioPairing[];
  countyPage: { name: string; href: string } | null;
  exploreLink: (measure: ResolvedPlaceMeasure, place: LevelPlace) => string;
}) {
  const own = levels[0];
  const countyGeoId = level === "COUNTY" && own?.level === "COUNTY" ? own.geoId : "";
  const pairing = useMemo(() => agencyRateFor(pairings, VIOLENT_RATE), [pairings]);
  const [crime, setCrime] = useState<CountyCrimeHistory | null>(null);

  useEffect(() => {
    if (!countyGeoId || !pairing) {
      setCrime(null);
      return;
    }
    const controller = new AbortController();
    const params = {
      ratio_id: pairing.ratio_id,
      geo_id: countyGeoId,
      scope: "as_released",
      newest_release_per_period: true,
      limit: CRIME_HISTORY_LIMIT,
    };
    setCrime({ state: "reading" });
    getDerivedObservations(params, { signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        const rows = (Array.isArray(payload?.items) ? payload.items : []).filter((row) => row.geo_id === countyGeoId);
        setCrime({ state: "read", rows, apiPath: buildApiPath("/observations/derived", params) });
      })
      .catch((error) => {
        if (!controller.signal.aborted) setCrime({ state: "failed", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, [countyGeoId, pairing]);

  const answered = useMemo(
    () => recordAnswers({ level, levels, shown, histories, answers, countyCrime: crime, countyPage, exploreLink }),
    [level, levels, shown, histories, answers, crime, countyPage, exploreLink],
  );
  if (!own || !answered.length) return null;

  return (
    <section className="analysis-panel place-record" id="record" aria-labelledby="record-heading" data-testid="place-record">
      <div className="panel-heading">
        <div>
          <h2 id="record-heading">What the record shows</h2>
          <p className="subtle">
            Common questions about {own.name}, each answered from published figures: two periods, their values and the change, with
            the chart, the sources and the caveats beside it. Every sentence follows fixed wording rules: a change is called a rise or a
            fall only when the figures show one beyond their margins, and nothing is said about why.
          </p>
        </div>
      </div>
      <div className="place-record-grid">
        {answered.map((answer) => <RecordCard key={answer.id} answer={answer} />)}
      </div>
      <p className="subtle">
        <Link className="text-link" href="/data#rules">The rules every number here follows</Link>
      </p>
    </section>
  );
}

function RecordCard({ answer }: { answer: RecordAnswer }) {
  const testId = `record-${answer.id}`;
  return (
    <article className="place-record-card" data-testid={testId} data-status={answer.status} aria-labelledby={`${testId}-question`}>
      <h3 id={`${testId}-question`}>{answer.question}</h3>
      <div data-testid={`${testId}-answer`}>
        {answer.sentences.map((sentence) => <p key={sentence} className="place-record-sentence">{sentence}</p>)}
      </div>
      {answer.coverage.length ? (
        <ul className="place-record-coverage" data-testid={`${testId}-coverage`}>
          {answer.coverage.map((line) => <li key={line}>{line}</li>)}
        </ul>
      ) : null}
      {answer.charts.map((chart) => (
        <div key={chart.id} className="place-record-chart">
          <h4 className="place-record-chart-title">{chart.label}</h4>
          <PlaceTrend
            model={chart.model}
            label={chart.label}
            unit={chart.unit}
            notes={chart.notes}
            testId={`${testId}-chart-${chart.id}`}
          />
        </div>
      ))}
      {answer.sources.length || answer.caveats.length ? (
        <details className="place-record-working" data-testid={`${testId}-working`}>
          <summary>Sources and caveats</summary>
          {answer.sources.length ? (
            <ul data-testid={`${testId}-sources`}>
              {answer.sources.map((source) => (
                <li key={source.metricCode}>
                  {source.label}
                  <span className="subtle" data-provenance="" data-metric-code={source.metricCode}> ({source.metricCode})</span>
                </li>
              ))}
            </ul>
          ) : null}
          {answer.caveats.length ? (
            <ul data-testid={`${testId}-caveats`}>
              {answer.caveats.map((caveat) => <li key={caveat}>{caveat}</li>)}
            </ul>
          ) : null}
        </details>
      ) : null}
      {answer.links.length ? (
        <p className="place-record-links">
          {answer.links.map((link) =>
            // An API read is a plain link: the router does not serve it.
            link.href.startsWith("/api/") ? (
              <a key={link.href} className="text-link" href={link.href} data-testid={`${testId}-link`}>{link.label}</a>
            ) : (
              <Link key={link.href} className="text-link" href={link.href} data-testid={`${testId}-link`}>{link.label}</Link>
            ),
          )}
        </p>
      ) : null}
    </article>
  );
}
