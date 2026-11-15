// "What stands out" on a place page: where the place ranks among its
// siblings, measure by measure (distinctive measures). Loaded with its own
// chunk so the place-page bundle carries only the import.

import Link from "next/link";

import { rankSentence, standoutName, standouts } from "../lib/distinctive";
import type { DistinctiveMeasure, DistinctiveResponse } from "../lib/distinctive";
import { formatObservationValue, unitText, valueWithUnit } from "../lib/explorerViewModel";
import { plainPeriodStart } from "../lib/plainPeriod";

function StandoutRow({ measure, siblingsName, low }: { measure: DistinctiveMeasure; siblingsName: string; low?: boolean }) {
  // The unit sits under the number and wraps there: "per 1,000 people" ran
  // past the panel edge when it shared the number's unbroken line.
  const unit = measure.units && measure.units !== "value" ? unitText(measure.units) : "";
  const shown = valueWithUnit(formatObservationValue(measure.value), unit);
  // "$" and "%" join the number; a unit in words stays on its own line.
  const [value, ...rest] = shown.split(" ");
  const words = rest.join(" ");
  return (
    <li data-testid={`standout-${measure.metric_code}`}>
      <Link className="text-link" href={`/map/${encodeURIComponent(measure.metric_code)}`} title={`See every county on a map · ${measure.metric_code}`}>{standoutName(measure)}</Link>
      <span className="standout-track" aria-hidden="true"><i style={{ left: `${Math.min(100, Math.max(0, measure.percentile_rank * 100))}%` }} /></span>
      <span className="standout-value" title={shown}><b>{value}</b>{words ? <small> {words}</small> : null}</span>
      <p className="subtle">{rankSentence(measure, siblingsName, low)}{measure.period_start ? `, period beginning ${plainPeriodStart(measure.period_start)}` : ""}.</p>
    </li>
  );
}

export default function WhatStandsOut({ response, siblingsName }: { response: DistinctiveResponse; siblingsName: string }) {
  const { highest, lowest, rankedCount } = standouts(response);
  const caveats = [...new Set([...highest, ...lowest].flatMap((measure) => measure.caveats))];
  return (
    <section className="analysis-panel place-standout" aria-labelledby="standout-heading" data-testid="place-standout">
      <h2 id="standout-heading">What stands out</h2>
      <p className="subtle" data-testid="place-standout-count">
        {rankedCount} measure{rankedCount === 1 ? "" : "s"} could be ranked among {siblingsName}, each on its own. The dot
        shows where this place falls from lowest to highest, not how far apart the values are; nothing here adds
        measures together, and places whose margins of error overlap may not truly differ. Each name opens its map.
      </p>
      <div className="place-standout-groups">
        <div>
          <h3>Among the highest</h3>
          <ul data-testid="place-standout-highest">{highest.map((measure) => <StandoutRow key={measure.metric_code} measure={measure} siblingsName={siblingsName} />)}</ul>
        </div>
        {lowest.length ? (
          <div>
            <h3>Among the lowest</h3>
            <ul data-testid="place-standout-lowest">{lowest.map((measure) => <StandoutRow key={measure.metric_code} measure={measure} siblingsName={siblingsName} low />)}</ul>
          </div>
        ) : null}
      </div>
      {caveats.length ? (
        <details className="subtle" data-testid="place-standout-caveats">
          <summary>Notes from the sources</summary>
          {caveats.map((caveat) => <p key={caveat}>{caveat}</p>)}
        </details>
      ) : null}
    </section>
  );
}

