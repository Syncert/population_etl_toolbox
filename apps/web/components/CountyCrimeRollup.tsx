"use client";

// The derived county crime roll-up (API-163 over warehouse ETL-053).
//
// The FBI publishes no county figures for the summarized products; the
// warehouse publishes a *declared-derived* sum of agency reports per
// county, measure, and month, and this panel is the one place a county
// profile offers it. It is deliberately separate from the provider-
// published measure cards above it, loads only on an explicit run, and
// renders every honesty the contract carries: derivation labeling,
// contributing agencies, reporting-versus-mapped coverage, the
// multi-county whole-count flag, and the non-additivity caveats. A county
// no resolved mapping covers surfaces the API's explicit refusal rather
// than an empty table.

import { useEffect, useRef, useState } from "react";
import { apiErrorMessage, apiFetch, buildApiPath } from "../lib/api/client";
import { formatNumber } from "../lib/format";

type RollupRow = {
  product_id: string;
  release: string;
  offense_label: string;
  measure_id: string;
  measure_form: string;
  counted_entity_basis: string;
  unit: string;
  geo_id: string;
  county_name: string | null;
  period: string;
  period_start: string;
  value: string;
  contributing_oris: string[];
  reporting_agency_count: number;
  mapped_agency_count: number;
  includes_multi_county_agency: boolean;
  derived: true;
  result_label: string;
  methodology_note: string;
};

type RollupResponse = {
  derived: true;
  release_selection: string;
  caveats: string[];
  total: number;
  limit: number;
  offset: number;
  items: RollupRow[];
};

const PRODUCT_ID = "summarized_violent_crime";
const PAGE_LIMIT = 200;

export default function CountyCrimeRollup({
  geoId,
  placeName,
}: {
  geoId: string;
  placeName: string;
}) {
  const [result, setResult] = useState<RollupResponse | null>(null);
  const [status, setStatus] = useState(
    "Load the derived roll-up to sum this county's agency reports.",
  );
  const [busy, setBusy] = useState(false);
  const controller = useRef<AbortController | null>(null);

  useEffect(() => {
    controller.current?.abort();
    setResult(null);
    setBusy(false);
    setStatus("Load the derived roll-up to sum this county's agency reports.");
    return () => controller.current?.abort();
  }, [geoId]);

  const params = { product_id: PRODUCT_ID, geo_id: geoId, limit: PAGE_LIMIT };
  const query = buildApiPath("/crime/county-rollup", params);

  async function run() {
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    setBusy(true);
    setResult(null);
    setStatus("Summing this county's published agency reports…");
    try {
      const response = await apiFetch<RollupResponse>("/crime/county-rollup", {
        params,
        signal: request.signal,
      });
      if (request.signal.aborted) return;
      if (
        response.derived !== true ||
        response.items.some(
          (row) => row.geo_id !== geoId || row.derived !== true,
        )
      ) {
        throw new Error(
          "The roll-up does not match the requested county or lost its derivation labeling; the answer was refused.",
        );
      }
      setResult(response);
      setStatus(
        response.items.length === 0
          ? "The mapped agencies published no reports for this county yet; nothing is shown as zero."
          : `${response.items.length} derived county-month rows${response.total > response.items.length ? ` of ${response.total}` : ""}. Every value is a sum of agency reports, not a provider-published county figure.`,
      );
    } catch (error) {
      // A county nothing maps to answers an explicit 404 refusal; its
      // detail is the honest message and is shown as-is.
      if (!request.signal.aborted) setStatus(apiErrorMessage(error));
    } finally {
      if (!request.signal.aborted) setBusy(false);
    }
  }

  const multiCountyContributors = result
    ? result.items.some((row) => row.includes_multi_county_agency)
    : false;

  return (
    <section
      className="analysis-panel county-crime-rollup"
      data-testid="county-crime-rollup"
      aria-labelledby="county-crime-rollup-title"
    >
      <div className="section-kicker">Derived county roll-up</div>
      <h2 id="county-crime-rollup-title">
        Sum this county&rsquo;s agency reports
      </h2>
      <p>
        The FBI publishes violent-crime counts per law-enforcement agency,
        not per county. The warehouse derives a county figure by summing the
        reports of agencies mapped to this county — a derived number with
        stated limits, not a provider-published total, and never a rate.
      </p>
      <div className="use-case-viz-controls">
        <button
          type="button"
          className="button primary"
          onClick={run}
          disabled={busy || !geoId}
        >
          Load derived county roll-up
        </button>
      </div>
      <p role="status">{status}</p>
      {result && result.items.length > 0 ? (
        <>
          <p data-testid="county-rollup-coverage">
            <strong>
              Coverage: {result.items[0]!.reporting_agency_count} of{" "}
              {result.items[0]!.mapped_agency_count} mapped agencies reported in{" "}
              {result.items[0]!.period}.
            </strong>{" "}
            An agency month the FBI did not publish is excluded from the sum,
            never counted as zero.
            {multiCountyContributors ? (
              <span data-testid="county-rollup-multi-county">
                {" "}
                At least one contributing agency serves more than one county
                and is counted in full in each, so this county&rsquo;s figures
                are not additive to state totals.
              </span>
            ) : null}
          </p>
          <div className="table-wrap use-case-table">
            <table>
              <caption>
                Derived roll-up of agency-reported totals for{" "}
                {result.items[0]!.county_name || placeName}; every row is a
                derived sum, not a provider observation. Release{" "}
                {result.items[0]!.release}.
              </caption>
              <thead>
                <tr>
                  <th>Period</th>
                  <th>Measure</th>
                  <th>Derived sum</th>
                  <th>Reporting / mapped agencies</th>
                  <th>Contributing agencies (ORI)</th>
                  <th>Multi-county contributor</th>
                </tr>
              </thead>
              <tbody>
                {result.items.map((row) => (
                  <tr key={`${row.measure_id}-${row.period}`}>
                    <td>{row.period}</td>
                    <td>
                      {row.offense_label} {row.counted_entity_basis} (derived
                      sum)
                    </td>
                    <td>
                      {formatNumber(row.value)} {row.unit}
                    </td>
                    <td>
                      {row.reporting_agency_count} / {row.mapped_agency_count}
                    </td>
                    <td>{row.contributing_oris.join(", ")}</td>
                    <td>{row.includes_multi_county_agency ? "Yes" : "No"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <ul data-testid="county-rollup-caveats">
            {result.caveats.map((caveat) => (
              <li key={caveat}>{caveat}</li>
            ))}
          </ul>
          <div className="use-case-tool-row">
            <a className="text-link" href={query}>
              Reproduce this roll-up in the API
            </a>
          </div>
        </>
      ) : null}
    </section>
  );
}
