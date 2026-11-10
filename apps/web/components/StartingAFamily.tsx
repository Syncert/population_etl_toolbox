"use client";

// "Starting a family" on a place page (starting-a-family-collection). The
// rows and their rules are `lib/familyCollection.ts`; this reads what the
// page has not already read and draws each row as the chapters draw a card:
// the value, its period, the levels on one scale, and the rank the API
// derives. Nothing is added up, and no row is called good or bad.
//
// It reads only once the page's own reads have settled, so a value another
// chapter already read (median home value, a parent's child poverty rate) is
// reused rather than asked for twice, and its reads queue behind the page's.

import { useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";
import type { MetricSummary } from "../lib/api/types";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { unitText } from "../lib/explorerViewModel";
import { getNewestValuesBatch, newestKey } from "../lib/newestValues";
import { normalizeObservationRows } from "../lib/observationAccess";
import { threeLevelCard } from "../lib/placeChapters";
import type { LevelAnswer, LevelPlace, PlaceLevel } from "../lib/placeChapters";
import { derivedMapRow, getDerivedObservations } from "../lib/derivedViews";
import type { DerivedRatioObservation, DerivedRatioPairing } from "../lib/derivedViews";
import { rankSentence } from "../lib/distinctive";
import type { DistinctiveResponse } from "../lib/distinctive";
import {
  FAMILY_GROUPS,
  FAMILY_INTRO,
  FAMILY_ROWS,
  familyValueText,
  headlineLevel,
  hoursFormula,
  hoursGap,
  hoursText,
  schoolsText,
} from "../lib/familyCollection";
import type { FamilyMinutes, FamilyRow } from "../lib/familyCollection";
import type { InfantCareRead } from "../lib/childcare";

const PlaceLevelStrip = dynamic(() => import("./PlaceLevelStrip"));

const NOT_LOADED = "Not loaded into this site yet.";
const key = (code: string, geoId: string) => `${code}|${geoId}`;
const GRAIN: Record<PlaceLevel, string> = {
  NATIONAL: "national",
  STATE: "state",
  METRO: "metro area",
  COUNTY: "county",
  PLACE: "city or town",
};

export default function StartingAFamily({
  levels,
  pageLevel,
  answers,
  settled,
  sources,
  metricsByCode,
  pairings,
  distinctive,
  siblingsName,
  infant,
}: {
  /** The page's chain of levels, its own place first. */
  levels: LevelPlace[];
  pageLevel: PlaceLevel;
  /** The page's own answers, keyed `metric|geo`; reused where they hold a row. */
  answers: ReadonlyMap<string, LevelAnswer>;
  /** The page's reads have settled. */
  settled: boolean;
  sources: readonly ExplorerSource[];
  metricsByCode: ReadonlyMap<string, MetricSummary>;
  pairings: readonly DerivedRatioPairing[];
  distinctive: DistinctiveResponse | null;
  siblingsName: string;
  /**
   * On a county page, what the childcare card read for center infant care:
   * that price and its hours are taken from it rather than asked again.
   */
  infant?: InfantCareRead;
}) {
  const [read, setRead] = useState<Map<string, LevelAnswer>>(new Map());
  const [hours, setHours] = useState<Map<string, FamilyMinutes | null>>(new Map());
  // A ratio over schools: how many of each place's schools it covers.
  const [schools, setSchools] = useState<Map<string, { withValue?: string | null; withoutValue?: string | null }>>(new Map());
  const served = useMemo(() => new Set(pairings.filter((pairing) => pairing.served).map((pairing) => pairing.ratio_id)), [pairings]);
  const chainKey = levels.map((entry) => `${entry.level}:${entry.geoId}`).join(",");
  const sharesInfant = infant !== undefined;
  // The childcare card's own price, taken as it is rather than read again.
  const fromCard = (row: FamilyRow, entry: LevelPlace) => sharesInfant && row.id === "infant-care" && entry.level === "COUNTY";

  // Whether each row is loaded: a metric the catalog publishes, or a pairing
  // the API serves. A row that is neither says so and reads nothing.
  const loaded = (row: FamilyRow): boolean =>
    row.metricCode ? metricsByCode.has(row.metricCode) : Boolean(row.ratioId && served.has(row.ratioId));

  // The values the page has not read, once it has settled.
  useEffect(() => {
    if (!settled || !levels.length) return;
    const controller = new AbortController();
    const signal = controller.signal;
    const next = new Map<string, LevelAnswer>();
    const missing: { code: string; geoId: string }[] = [];
    for (const row of FAMILY_ROWS) {
      if (!loaded(row)) continue;
      for (const entry of levels.filter((item) => row.levels.includes(item.level))) {
        if (fromCard(row, entry)) continue;
        if (row.metricCode) {
          const known = answers.get(key(row.metricCode, entry.geoId));
          if (known && !known.pending && (known.row || known.error)) next.set(key(row.metricCode, entry.geoId), known);
          else {
            next.set(key(row.metricCode, entry.geoId), { row: null, pending: true });
            missing.push({ code: row.metricCode, geoId: entry.geoId });
          }
        } else if (row.ratioId) {
          next.set(key(row.ratioId, entry.geoId), { row: null, pending: true });
        }
      }
    }
    setRead(new Map(next));
    const nextSchools = new Map<string, { withValue?: string | null; withoutValue?: string | null }>();
    setSchools(new Map());
    const sourceFor = (code: string) => sources.find((source) => source.sourceCode === metricsByCode.get(code)?.source_code) || null;
    const tasks: Promise<void>[] = [];
    if (missing.length) {
      tasks.push(
        getNewestValuesBatch(missing.map((pair) => pair.code), missing.map((pair) => pair.geoId), { signal })
          .then((found) => {
            for (const { code, geoId } of missing) {
              const item = found.get(newestKey(code, geoId));
              const rows = item?.status === 200 && item.observations
                ? normalizeObservationRows(sourceFor(code), Array.isArray(item.observations.items) ? (item.observations.items as ObservationRow[]) : [])
                : [];
              next.set(key(code, geoId), item && item.status !== 200 ? { row: null, error: item.detail || `Could not be read (${item.status})` } : { row: rows.at(-1) ?? null });
            }
          })
          .catch(() => {
            if (signal.aborted) return;
            for (const { code, geoId } of missing) next.set(key(code, geoId), { row: null, error: "Could not be read just now" });
          }),
      );
    }
    for (const row of FAMILY_ROWS) {
      if (!row.ratioId || !loaded(row)) continue;
      for (const entry of levels.filter((item) => row.levels.includes(item.level))) {
        tasks.push(
          getDerivedObservations({ ratio_id: row.ratioId, geo_id: entry.geoId, newest_per_geography: true, limit: 1 }, { signal })
            .then((page) => {
              const found = (page.items || []).find((item: DerivedRatioObservation) => item.geo_id === entry.geoId) || null;
              next.set(key(row.ratioId!, entry.geoId), { row: found ? derivedMapRow(found, row.ratioId!) : null });
              if (found?.numerator?.schools_with_value != null && !found.unavailable_reason) {
                nextSchools.set(key(row.ratioId!, entry.geoId), {
                  withValue: found.numerator.schools_with_value,
                  withoutValue: found.numerator.schools_without_value,
                });
              }
            })
            .catch(() => {
              if (!signal.aborted) next.set(key(row.ratioId!, entry.geoId), { row: null, error: "Could not be read just now" });
            })
            .finally(() => {
              if (signal.aborted) return;
              setRead(new Map(next));
              setSchools(new Map(nextSchools));
            }),
        );
      }
    }
    void Promise.all(tasks).then(() => {
      if (!signal.aborted) setRead(new Map(next));
    });
    return () => controller.abort();
    // `loaded` reads only metricsByCode and served; `answers` is read once settled.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [settled, chainKey, metricsByCode, served, sources, sharesInfant]);

  // Hours of work for each price row, at the county's own wage, once its
  // price is in: the price's own period, so the hours are of the price shown.
  const county = levels.find((entry) => entry.level === "COUNTY") || null;
  const priceRows = FAMILY_ROWS.filter((row) => row.hoursRatioId && row.metricCode);
  const pricePeriods = county
    ? priceRows.map((row) => String(read.get(key(row.metricCode!, county.geoId))?.row?.period_start || "")).join(",")
    : "";
  useEffect(() => {
    if (!county || !pricePeriods.replace(/,/g, "")) return;
    const controller = new AbortController();
    priceRows.forEach((row, index) => {
      const period = pricePeriods.split(",")[index];
      if (!period || !served.has(row.hoursRatioId!) || fromCard(row, county)) return;
      getDerivedObservations(
        { ratio_id: row.hoursRatioId!, geo_id: county.geoId, wage_geo_id: county.geoId, period_start: period.slice(0, 10), limit: 1 },
        { signal: controller.signal },
      )
        .then((page) => {
          if (!controller.signal.aborted) setHours((current) => new Map(current).set(row.id, (page.items[0] as FamilyMinutes) || null));
        })
        .catch(() => {
          if (!controller.signal.aborted) setHours((current) => new Map(current).set(row.id, null));
        });
    });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [county?.geoId, pricePeriods, served]);

  const shown = useMemo(() => {
    if (!infant || !county) return read;
    const merged = new Map(read);
    merged.set(
      key("DOL_CHILDCARE:center_infant", county.geoId),
      infant.row === undefined
        ? { row: null, pending: true }
        : infant.error && infant.error !== "unloaded"
          ? { row: null, error: infant.error }
          : { row: infant.row ?? null },
    );
    return merged;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [read, infant, county?.geoId]);
  const minutesFor = (row: FamilyRow): FamilyMinutes | null | undefined =>
    row.id === "infant-care" && infant && county ? (infant.minutes as FamilyMinutes | null | undefined) : hours.get(row.id);

  if (!levels.length) return null;
  const formulas = priceRows
    .map((row) => (county ? hoursFormula(minutesFor(row), row, county.name) : ""))
    .filter(Boolean);

  return (
    <section className="analysis-panel place-chapter place-family" id="family" aria-labelledby="family-heading" data-testid="chapter-family">
      <div className="panel-heading">
        <div>
          <h2 id="family-heading">Starting a family</h2>
          <p className="subtle">{FAMILY_INTRO}</p>
        </div>
      </div>
      {FAMILY_GROUPS.map((group) => (
        <div className="place-family-group" key={group.key} data-testid={`family-group-${group.key}`}>
          <h3>{group.title}</h3>
          <ul className="place-family-rows">
            {FAMILY_ROWS.filter((row) => row.group === group.key).map((row) => (
              <FamilyLine
                key={row.id}
                row={row}
                levels={levels}
                pageLevel={pageLevel}
                loaded={loaded(row)}
                answers={shown}
                minutes={minutesFor(row)}
                schools={schools}
                county={county}
                distinctive={distinctive}
                siblingsName={siblingsName}
              />
            ))}
          </ul>
        </div>
      ))}
      <footer className="place-chapter-footer" data-testid="chapter-family-footer">
        {formulas.length ? (
          <p data-testid="family-hours-formula">
            <strong>How the hours are worked out:</strong> each price ÷ the county&apos;s average hourly wage (its QCEW average weekly
            wage for all jobs located in the county ÷ {40} hours). {formulas.join(" ")}
          </p>
        ) : null}
        <p>
          Sources: U.S. Department of Labor Women&apos;s Bureau (childcare), HUD (Fair Market Rents), Census Bureau ACS and SAIPE,
          Bureau of Labor Statistics QCEW (wages), NCES Common Core of Data (schools). Each line&apos;s period is its own; a figure
          published only for a larger area is that area&apos;s, named, and nothing is estimated for a level a source does not publish.
        </p>
      </footer>
    </section>
  );
}

function FamilyLine({
  row,
  levels,
  pageLevel,
  loaded,
  answers,
  minutes,
  county,
  distinctive,
  siblingsName,
  schools,
}: {
  row: FamilyRow;
  levels: LevelPlace[];
  pageLevel: PlaceLevel;
  loaded: boolean;
  answers: ReadonlyMap<string, LevelAnswer>;
  minutes: FamilyMinutes | null | undefined;
  county: LevelPlace | null;
  distinctive: DistinctiveResponse | null;
  siblingsName: string;
  schools: ReadonlyMap<string, { withValue?: string | null; withoutValue?: string | null }>;
}) {
  const testId = `family-${row.id}`;
  if (!loaded) {
    return (
      <li className="place-family-row" data-testid={testId} data-available="false">
        <h4>{row.label}</h4>
        <p className="subtle">{NOT_LOADED}</p>
      </li>
    );
  }
  const code = row.metricCode || row.ratioId!;
  const byGeo = new Map(levels.map((entry) => [entry.geoId, answers.get(key(code, entry.geoId)) || { row: null, pending: true }]));
  const card = threeLevelCard(levels, byGeo, (level) => row.levels.includes(level));
  const lead = headlineLevel(row, levels);
  const leadRow = lead ? card.rows.find((entry) => entry.place.geoId === lead.geoId) : null;
  const unit = unitText(leadRow?.row?.unit || leadRow?.row?.units) || row.unit || "";
  const value = leadRow?.row && !leadRow.message ? familyValueText(leadRow.row.value, unit) : "";
  const notOwn = lead && lead.level !== pageLevel;
  const ranked = lead && lead.level === pageLevel && row.metricCode
    ? distinctive?.ranked.find((measure) => measure.metric_code === row.metricCode && distinctive.geo_id === lead.geoId) || null
    : null;
  const hoursLine = row.hoursRatioId && county && lead?.geoId === county.geoId ? hoursText(minutes, row.hoursSameYearOnly) : "";
  const coverage = row.ratioId
    ? schoolsText(card.rows.filter((entry) => entry.row && !entry.message).map((entry) => ({ name: entry.place.name, ...schools.get(key(code, entry.place.geoId)) })))
    : "";
  const gap = row.hoursRatioId && county && lead?.geoId === county.geoId && value ? hoursGap(minutes, row, county.name) : "";
  return (
    <li className="place-family-row" data-testid={testId} data-available="true" data-period={card.period}>
      <h4>{row.label}</h4>
      <p className="place-family-value">
        {value ? (
          <>
            <strong data-testid={`${testId}-value`}>{value}</strong>
            {notOwn ? <span className="subtle"> · {lead!.name} figure</span> : null}
            {card.period ? <span className="subtle"> · {card.period}</span> : null}
          </>
        ) : !lead ? (
          <span className="subtle" data-testid={`${testId}-gap`}>{row.gap || `Not published at ${GRAIN[pageLevel]} grain`}</span>
        ) : (
          <span className="subtle" data-testid={`${testId}-gap`}>{leadRow?.message || "Reading…"}</span>
        )}
      </p>
      {hoursLine ? <p className="place-family-hours" data-testid={`${testId}-hours`}>{hoursLine} at {county!.name}&apos;s average wage</p> : null}
      {gap ? <p className="subtle" data-testid={`${testId}-hours-gap`}>{gap}</p> : null}
      <PlaceLevelStrip measureId={`family-${row.id}`} rows={card.rows} />
      {coverage ? <p className="subtle" data-testid={`${testId}-schools`}>{coverage}</p> : null}
      {ranked ? <p className="subtle" data-testid={`${testId}-rank`}>{rankSentence(ranked, siblingsName)}</p> : null}
      {row.gap && levels.some((entry) => !row.levels.includes(entry.level)) && lead ? (
        <p className="subtle" data-testid={`${testId}-levels`}>{row.gap}.</p>
      ) : null}
      {row.note ? <p className="place-basis">{row.note}</p> : null}
    </li>
  );
}
