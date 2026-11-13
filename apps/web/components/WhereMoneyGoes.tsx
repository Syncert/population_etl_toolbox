"use client";

/**
 * Where the money goes (where-public-money-goes slice 5).
 *
 * The Work and Money chapter's group for the nation, a state or a county:
 * one card per government level, each with its largest spending lines as
 * shares of that level's own total, the year, the source and its notes.
 * The levels are separate figures and are never added: federal awards
 * recorded in a county are not what its local governments spend. Loaded as
 * its own chunk, so place pages pay nothing until it renders.
 */

import { useEffect, useState } from "react";

import { ApiError, apiErrorMessage, apiFetch } from "../lib/api/client";
import {
  WHERE_MONEY_GEO_ID,
  dollars,
  layerHeading,
  moneyText,
  shareBars,
  shareText,
  totalSentence,
  unsharedReason,
} from "../lib/whereMoneyGoes";
import type { SpendingLayer, WhereMoneyGoesAnswer } from "../lib/whereMoneyGoes";

type Load =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; answer: WhereMoneyGoesAnswer | null };

const NOT_LOADED = "Not loaded into this site yet.";

function LineTable({ layer }: { layer: SpendingLayer }) {
  return (
    <table className="where-money-table">
      <thead>
        <tr>
          <th scope="col">Line</th>
          <th scope="col">Amount</th>
          <th scope="col">Share</th>
        </tr>
      </thead>
      <tbody>
        {layer.lines.map((line) => (
          <tr key={line.key}>
            <th scope="row">{line.label}</th>
            <td title={line.amount ?? undefined}>{moneyText(dollars(line.amount, layer.unit))}</td>
            <td>{shareText(line.share_pct)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function Layer({ layer }: { layer: SpendingLayer }) {
  const testId = `where-money-${layer.layer}`;
  const bars = shareBars(layer);
  const total = totalSentence(layer);
  const unshared = unsharedReason(layer);
  const max = bars.length ? Math.max(...bars.map((bar) => bar.share)) : 0;
  return (
    <div className="public-money-group where-money-layer" data-testid={testId}>
      <h4>{layerHeading(layer)}</h4>
      {total ? <p className="public-money-sentence" data-testid={`${testId}-total`}>{total}</p> : null}
      {bars.length ? (
        <table className="where-money-bars" data-testid={`${testId}-bars`}>
          <caption className="sr-only">{`${layer.title}: the largest lines as a share of ${layer.total_label.toLowerCase()}`}</caption>
          <colgroup>
            <col className="where-money-col-label" />
            <col />
            <col className="where-money-col-share" />
            <col className="where-money-col-amount" />
          </colgroup>
          <tbody>
            {bars.map((bar) => (
              <tr key={bar.key} data-testid={`${testId}-bar-${bar.key}`} className={bar.rest ? "where-money-rest" : undefined}>
                <th scope="row">{bar.label}</th>
                <td>
                  <span className="where-money-track" aria-hidden="true">
                    <span className="where-money-fill" style={{ width: `${max ? (bar.share / max) * 100 : 0}%` }} />
                  </span>
                </td>
                <td className="public-money-value">{shareText(String(bar.share))}</td>
                <td className="where-money-amount">{moneyText(bar.amount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : layer.lines.length ? null : (
        <p className="subtle" data-testid={`${testId}-empty`}>
          {layer.unavailable_reason && layer.unavailable_reason !== "not loaded into this site yet"
            ? `Not published for this place${layer.fiscal_year ? ` in fiscal ${layer.fiscal_year}` : ""}.`
            : NOT_LOADED}
        </p>
      )}
      {unshared && layer.lines.length ? (
        <p className="subtle" data-testid={`${testId}-unshared`}>
          {total ? "Some lines have no share" : "No share is shown"}: {unshared}.
        </p>
      ) : null}
      {layer.lines.length ? (
        bars.length ? (
          <details className="where-money-all" data-testid={`${testId}-all`}>
            <summary>{`All ${layer.lines.length} line${layer.lines.length === 1 ? "" : "s"}`}</summary>
            <LineTable layer={layer} />
          </details>
        ) : (
          // No share to draw: the amounts are the answer, shown open.
          <div className="where-money-all" data-testid={`${testId}-all`}>
            <LineTable layer={layer} />
          </div>
        )
      ) : null}
      <p className="place-basis">{layer.source}</p>
      {layer.notes.map((note) => (
        <p key={note} className="subtle">{note}</p>
      ))}
    </div>
  );
}

export default function WhereMoneyGoes({ geoId }: { geoId: string }) {
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const served = WHERE_MONEY_GEO_ID.test(geoId);

  useEffect(() => {
    if (!served) return;
    let cancelled = false;
    setLoad({ state: "loading" });
    apiFetch<WhereMoneyGoesAnswer>("/where-money-goes", { params: { geo_id: geoId } })
      .then((answer) => {
        if (!cancelled) setLoad({ state: "ready", answer });
      })
      .catch((error) => {
        if (cancelled) return;
        if (error instanceof ApiError && error.status === 404) setLoad({ state: "ready", answer: null });
        else setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => {
      cancelled = true;
    };
  }, [geoId, served]);

  if (!served) return null;
  if (load.state === "loading") return <p className="subtle" data-testid="where-money-loading">Reading where the money goes…</p>;
  if (load.state === "error") return <p className="subtle" data-testid="where-money-error">Where the money goes could not be read: {load.message}</p>;
  const answer = load.answer;
  return (
    <section className="public-money where-money" aria-labelledby="where-money-heading" data-testid="where-money">
      <h3 id="where-money-heading">Where the money goes</h3>
      <p className="subtle">Spending by each level of government, as each source publishes it. Each share is of its own level&apos;s total, for one year.</p>
      {answer && answer.layers.length ? (
        <>
          {answer.layers.map((layer) => <Layer key={layer.layer} layer={layer} />)}
          <p className="public-money-sentence where-money-separate" data-testid="where-money-separate">{answer.caveats[0]}</p>
        </>
      ) : (
        <p className="subtle" data-testid="where-money-empty">{NOT_LOADED}</p>
      )}
    </section>
  );
}
