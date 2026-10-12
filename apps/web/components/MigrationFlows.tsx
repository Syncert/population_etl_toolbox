"use client";

/**
 * Where a county's movers came from and went (irs-county-migration).
 *
 * Reads `/api/v1/migration-flows` for the newest published pair of filing
 * years, once per direction, and lists the county-to-county flows as SOI
 * published them, with SOI's own categories named beside them. A category
 * SOI deleted is said to be withheld, never shown as zero, and nothing
 * here computes a net figure: these are tax filers, not the population
 * estimates the Change chapter shows.
 */

import { useEffect, useState } from "react";

import { ApiError, apiErrorMessage, apiFetch } from "../lib/api/client";
import { formatObservationValue } from "../lib/explorerViewModel";
import { MIGRATION_POPULATION_NOTE } from "../lib/placeChapters";

const LIST_LENGTH = 10;

export interface MigrationRow {
  category: string;
  category_label: string;
  counterpart_geo_id?: string | null;
  counterpart_name?: string | null;
  returns?: number | null;
  value_status: string;
}

export interface MigrationAnswer {
  direction: "inflow" | "outflow";
  year_pair: string;
  total: number;
  items: MigrationRow[];
  categories: MigrationRow[];
  totals: MigrationRow[];
}

type Load =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; inflow: MigrationAnswer | null; outflow: MigrationAnswer | null };

async function read(geoId: string, direction: "inflow" | "outflow"): Promise<MigrationAnswer | null> {
  try {
    return await apiFetch<MigrationAnswer>("/migration-flows", {
      params: { geo_id: geoId, direction, limit: LIST_LENGTH },
    });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}

/** How many of SOI's categories this answer withheld. */
export function withheldCategories(answer: MigrationAnswer): MigrationRow[] {
  return answer.categories.filter((row) => row.value_status === "withheld");
}

function FlowList({ answer, heading, testId }: { answer: MigrationAnswer; heading: string; testId: string }) {
  const withheld = withheldCategories(answer);
  const total = answer.totals.find((row) => row.category === "total_us_and_foreign");
  return (
    <div className="place-migration-list" data-testid={testId}>
      <h3>{heading}</h3>
      <p className="subtle">
        Tax returns, {answer.year_pair} filing years
        {total && total.returns !== null && total.returns !== undefined
          ? `; ${formatObservationValue(total.returns)} in all, as SOI totals them`
          : ""}
        .
      </p>
      {answer.items.length ? (
        <ol>
          {answer.items.map((row) => (
            <li key={row.counterpart_geo_id || row.category} data-testid={`${testId}-${row.counterpart_geo_id}`}>
              <span>{row.counterpart_name || row.counterpart_geo_id}</span>{" "}
              <span className="place-migration-value">
                {row.returns === null || row.returns === undefined ? "withheld" : formatObservationValue(row.returns)}
              </span>
            </li>
          ))}
        </ol>
      ) : (
        <p>No single county reached SOI&apos;s 20-return threshold.</p>
      )}
      <p className="subtle" data-testid={`${testId}-withheld`}>
        Counties with fewer than 20 returns are grouped into SOI&apos;s &ldquo;Other flows&rdquo; categories
        {withheld.length
          ? `; ${withheld.map((row) => row.category_label).join(", ")} ${withheld.length === 1 ? "is" : "are"} withheld by SOI to protect taxpayers, not zero`
          : ""}
        .
      </p>
    </div>
  );
}

export default function MigrationFlows({ geoId }: { geoId: string }) {
  const [load, setLoad] = useState<Load>({ state: "loading" });

  useEffect(() => {
    let cancelled = false;
    setLoad({ state: "loading" });
    Promise.all([read(geoId, "inflow"), read(geoId, "outflow")])
      .then(([inflow, outflow]) => {
        if (!cancelled) setLoad({ state: "ready", inflow, outflow });
      })
      .catch((error) => {
        if (!cancelled) setLoad({ state: "error", message: apiErrorMessage(error) });
      });
    return () => {
      cancelled = true;
    };
  }, [geoId]);

  if (load.state === "loading") return <p className="subtle" data-testid="migration-loading">Reading IRS migration flows…</p>;
  if (load.state === "error") return <p className="subtle" data-testid="migration-error">IRS migration flows could not be read: {load.message}</p>;
  if (!load.inflow && !load.outflow) return null;
  return (
    <section className="place-migration" aria-label="Migration flows" data-testid="migration-flows">
      <div className="place-migration-lists">
        {load.inflow ? <FlowList answer={load.inflow} heading="Where people came from" testId="migration-inflow" /> : null}
        {load.outflow ? <FlowList answer={load.outflow} heading="Where people went" testId="migration-outflow" /> : null}
      </div>
      <p className="place-basis" data-testid="migration-basis">
        IRS Statistics of Income county-to-county migration, counted from tax returns
      </p>
      <p className="subtle" data-testid="migration-population-note">{MIGRATION_POPULATION_NOTE}</p>
    </section>
  );
}
