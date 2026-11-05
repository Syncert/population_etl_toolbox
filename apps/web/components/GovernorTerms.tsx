"use client";

/**
 * A state's governors with taxes and public money (taxes-and-public-money
 * T-6, ADR-0014 and its fiscal section).
 *
 * One row per governor: the years compared, each fiscal measure's change
 * over them minus the nation's (a neutral grey bar either side of zero,
 * never good or bad colours), and the tax mix in the last year compared.
 * Party colours mark party identity only. Loaded as its own chunk on state
 * pages.
 */

import { useEffect, useState } from "react";

import { ApiError, apiErrorMessage, apiFetch } from "../lib/api/client";
import {
  CHANGE_GREY,
  NOT_LOADED,
  STATE_GEO_ID,
  TAX_MIX_COLOURS,
  TAX_MIX_LABELS,
  TAX_MIX_ORDER,
  changeScale,
  gapText,
  numberOrNull,
  partyLetter,
  percentText,
  pointsText,
  tenureSpan,
  tenureYears,
} from "../lib/publicMoney";
import type { GovernorTenure, GovernorTermsAnswer } from "../lib/publicMoney";

type Load =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; answer: GovernorTermsAnswer | null };

function ChangeCell({ value, reason, scale, testId }: { value: string | null; reason: string | null; scale: number; testId: string }) {
  const number = numberOrNull(value);
  if (number === null) return <td className="governor-change subtle" data-testid={testId}>{gapText(reason)}</td>;
  const width = (Math.min(Math.abs(number), scale) / scale) * 50;
  return (
    <td className="governor-change" data-testid={testId}>
      <span className="governor-change-value">{pointsText(number)}</span>
      <span className="governor-change-track" aria-hidden="true">
        <span
          className="governor-change-bar"
          style={{ left: number < 0 ? `${50 - width}%` : "50%", width: `${width}%`, background: CHANGE_GREY }}
        />
      </span>
    </td>
  );
}

function MixCell({ tenure }: { tenure: GovernorTenure }) {
  if (!tenure.tax_mix.length) return <td className="governor-mix subtle">Not published</td>;
  const shares = TAX_MIX_ORDER.map((group) => tenure.tax_mix.find((entry) => entry.tax_group === group)).filter(Boolean);
  const text = shares.map((entry) => `${TAX_MIX_LABELS[entry!.tax_group]} ${percentText(entry!.share_pct)}`).join(", ");
  return (
    <td className="governor-mix" data-testid={`governor-mix-${tenure.tenure_key}`}>
      <span className="governor-mix-bar" aria-hidden="true" title={`Fiscal ${tenure.tax_mix_year}: ${text}`}>
        {shares.map((entry) => (
          <span key={entry!.tax_group} style={{ flexGrow: Math.max(0, numberOrNull(entry!.share_pct) ?? 0), background: TAX_MIX_COLOURS[entry!.tax_group] }} />
        ))}
      </span>
      <span className="sr-only">{`Fiscal ${tenure.tax_mix_year}: ${text}`}</span>
    </td>
  );
}

export default function GovernorTerms({ geoId, stateName }: { geoId: string; stateName: string }) {
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const served = STATE_GEO_ID.test(geoId);

  useEffect(() => {
    if (!served) return;
    let cancelled = false;
    setLoad({ state: "loading" });
    apiFetch<GovernorTermsAnswer>("/governor-terms", { params: { geo_id: geoId } })
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
  if (load.state === "loading") return <p className="subtle" data-testid="governor-terms-loading">Reading governors…</p>;
  if (load.state === "error") return <p className="subtle" data-testid="governor-terms-error">Governors could not be read: {load.message}</p>;
  const answer = load.answer;
  const items = answer?.items || [];
  const scale = changeScale(items);
  return (
    <section className="governor-terms" aria-labelledby="governor-terms-heading" data-testid="governor-terms">
      <h4 id="governor-terms-heading">{stateName} under each governor: taxes and public money against the nation</h4>
      {items.length ? (
        <>
          <p className="subtle">
            Each number is {stateName}&apos;s change over the years compared minus the nation&apos;s change over the same years, in percentage points.
            Grey bars: whether higher taxes or payments are better is the reader&apos;s call, not the site&apos;s.
          </p>
          <ul className="public-money-legend" data-testid="governor-mix-legend" aria-label="Tax mix">
            {TAX_MIX_ORDER.map((group) => (
              <li key={group}>
                <span className="public-money-swatch" style={{ background: TAX_MIX_COLOURS[group] }} aria-hidden="true" />
                {TAX_MIX_LABELS[group]}
              </li>
            ))}
          </ul>
          <div className="governor-table-wrap" role="region" aria-label={`Governors of ${stateName}`} tabIndex={0}>
            <table className="governor-table" data-testid="governor-table">
              <caption className="sr-only">Governors of {stateName} with tax and payment changes net of the nation</caption>
              <thead>
                <tr>
                  <th scope="col">Governor</th>
                  <th scope="col">Years compared</th>
                  {answer!.measures.map((measure) => (
                    <th key={measure.measure} scope="col">{measure.label}, points vs nation</th>
                  ))}
                  <th scope="col">Tax mix, last year compared</th>
                </tr>
              </thead>
              <tbody>
                {items.map((tenure) => (
                  <tr key={tenure.tenure_key} data-testid={`governor-${tenure.tenure_key}`}>
                    <th scope="row">
                      {tenure.holder_name}{" "}
                      <span className={`party-chip party-${partyLetter(tenure.party_code)}`} title={tenure.party_text || undefined}>
                        {partyLetter(tenure.party_code)}
                      </span>
                      <small className="subtle"> {tenureSpan(tenure)}</small>
                    </th>
                    <td>{tenureYears(tenure)}</td>
                    {answer!.measures.map((measure) => {
                      const change = tenure.fiscal.find((entry) => entry.measure === measure.measure);
                      return (
                        <ChangeCell
                          key={measure.measure}
                          value={change?.net_change ?? null}
                          reason={change?.unavailable_reason ?? null}
                          scale={scale}
                          testId={`governor-${tenure.tenure_key}-${measure.measure}`}
                        />
                      );
                    })}
                    <MixCell tenure={tenure} />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="place-basis">{answer!.measures.map((measure) => measure.source).join("; ")}</p>
          {answer!.caveats.map((caveat) => <p key={caveat} className="subtle">{caveat}</p>)}
        </>
      ) : (
        <p className="subtle" data-testid="governor-terms-empty">{NOT_LOADED}</p>
      )}
    </section>
  );
}
