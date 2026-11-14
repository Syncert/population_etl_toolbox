"use client";

/**
 * Taxes and public money (taxes-and-public-money T-6).
 *
 * The Work and Money chapter's group for the nation, a state or a county:
 * what residents receive in payments from government, where their income
 * comes from, the federal income tax rate on their returns, the state and
 * local taxes collected, and the property tax load -- each against the state
 * and the nation, each measure on its own (ADR-0014 fiscal section). Every
 * share was computed by the API under an approved pairing; this draws it.
 * Loaded as its own chunk, so place pages pay nothing until it renders.
 */

import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { ApiError, apiErrorMessage, apiFetch } from "../lib/api/client";
import {
  LEVEL_COLOURS,
  NOT_LOADED,
  PUBLIC_MONEY_GEO_ID,
  PUBLIC_MONEY_TERMS,
  incomeRows,
  lineGeometry,
  linePath,
  lineSeries,
  nationGapSentence,
  paymentRows,
  percentText,
  propertyRow,
  spreadLabels,
  stripMax,
} from "../lib/publicMoney";
import type { LineSeries, PublicMoneyAnswer, PublicMoneyLevel, StripRow } from "../lib/publicMoney";

type Load =
  | { state: "loading" }
  | { state: "error"; message: string }
  | { state: "ready"; answer: PublicMoneyAnswer | null };

/** Where each map lives: a measure's own map with its approved share view. */
export const PAYMENTS_MAP = `/map/${encodeURIComponent("BEA:CAINC35:1000")}?view=share`;
export const TAX_RATE_MAP = `/map/${encodeURIComponent("IRS_SOI:income_tax_amount")}?view=per_capita`;

function Legend({ levels, names }: { levels: PublicMoneyLevel[]; names: Map<string, string> }) {
  return (
    <ul className="public-money-legend" data-testid="public-money-legend">
      {levels.map((level) => (
        <li key={level.geo_id}>
          <span className="public-money-swatch" style={{ background: LEVEL_COLOURS[level.role] }} aria-hidden="true" />
          {names.get(level.geo_id) || level.geo_id}
        </li>
      ))}
    </ul>
  );
}

/** Rows of dot strips on one shared scale, the site's level-strip vocabulary. */
function Strips({ rows, names, testId, caption }: { rows: StripRow[]; names: Map<string, string>; testId: string; caption: string }) {
  const max = stripMax(rows);
  return (
    <table className="public-money-strips" data-testid={testId}>
      <caption className="sr-only">{caption}</caption>
      <tbody>
        {rows.map((row) => {
          const own = row.values[0];
          return (
            <tr key={row.id} data-testid={`${testId}-${row.id}`} className={row.emphasis ? "public-money-emphasis" : undefined}>
              <th scope="row">{row.label}</th>
              <td>
                <span className="public-money-track" aria-hidden="true">
                  {row.values.map((entry, index) =>
                    entry.value === null ? null : (
                      <span
                        key={entry.geoId}
                        className="public-money-dot"
                        data-index={index}
                        title={`${names.get(entry.geoId) || entry.geoId}: ${percentText(entry.value)}`}
                        style={{ left: `${(entry.value / max) * 100}%`, background: LEVEL_COLOURS[entry.role] }}
                      />
                    ),
                  )}
                </span>
                <span className="sr-only">
                  {row.values.map((entry) => `${names.get(entry.geoId) || entry.geoId} ${percentText(entry.value)}`).join("; ")}
                </span>
              </td>
              <td className="public-money-value" aria-hidden="true">{percentText(own?.value ?? null)}</td>
            </tr>
          );
        })}
        <tr className="public-money-axis" aria-hidden="true">
          <th />
          <td>
            <span className="public-money-track">
              <small style={{ left: "0%" }}>0%</small>
              <small style={{ left: "100%" }}>{max}%</small>
            </span>
          </td>
          <td />
        </tr>
      </tbody>
    </table>
  );
}

const WIDTH = 560;
const HEIGHT = 180;
/** Room right of the plot for the end labels. */
const LABEL_ROOM = 150;

/** Lines for each level over the years, one axis from zero, labelled at their ends. */
function LevelLines({ series, names, testId, label }: { series: LineSeries[]; names: Map<string, string>; testId: string; label: string }) {
  const geometry = lineGeometry(series, WIDTH, HEIGHT);
  const [hover, setHover] = useState<number | null>(null);
  if (!geometry) return null;
  const [firstYear, lastYear] = geometry.years;
  const years = Array.from(new Set(series.flatMap((entry) => entry.points.map((point) => point.year)))).sort((a, b) => a - b);
  const labelYs = spreadLabels(series.map((entry) => geometry.y(entry.points[entry.points.length - 1]!.value) + 4));
  const readout = hover === null ? null : series.map((entry) => ({ entry, point: entry.points.find((point) => point.year === hover) })).filter((item) => item.point);
  const move = (event: React.MouseEvent<SVGSVGElement>) => {
    const box = event.currentTarget.getBoundingClientRect();
    const x = ((event.clientX - box.left) / box.width) * (WIDTH + LABEL_ROOM);
    let nearest = years[0]!;
    for (const year of years) if (Math.abs(geometry.x(year) - x) < Math.abs(geometry.x(nearest) - x)) nearest = year;
    setHover(nearest);
  };
  return (
    <figure className="public-money-lines" data-testid={testId}>
      <p className="public-money-readout subtle" aria-live="polite" data-testid={`${testId}-readout`}>
        {readout && readout.length
          ? `${hover}: ${readout.map(({ entry, point }) => `${names.get(entry.geoId) || entry.geoId} ${percentText(point!.value)}`).join(" · ")}`
          : " "}
      </p>
      <svg viewBox={`0 0 ${WIDTH + LABEL_ROOM} ${HEIGHT}`} role="img" aria-label={label} onMouseMove={move} onMouseLeave={() => setHover(null)}>
        {geometry.ticks.map((tick) => (
          <g key={tick}>
            <line x1={geometry.x(firstYear)} x2={geometry.x(lastYear)} y1={geometry.y(tick)} y2={geometry.y(tick)} className="public-money-grid" />
            <text x={geometry.x(firstYear) - 6} y={geometry.y(tick) + 4} textAnchor="end" className="public-money-tick">{tick}%</text>
          </g>
        ))}
        <text x={geometry.x(firstYear)} y={HEIGHT - 4} className="public-money-tick">{firstYear}</text>
        {lastYear !== firstYear ? <text x={geometry.x(lastYear)} y={HEIGHT - 4} textAnchor="end" className="public-money-tick">{lastYear}</text> : null}
        {hover !== null ? <line x1={geometry.x(hover)} x2={geometry.x(hover)} y1={geometry.y(geometry.values[1])} y2={geometry.y(0)} className="public-money-crosshair" /> : null}
        {series.map((entry, index) => {
          const last = entry.points[entry.points.length - 1]!;
          return (
            <g key={entry.geoId} data-testid={`${testId}-${entry.role.replace(" ", "-")}`}>
              <path d={linePath(entry.points, geometry)} fill="none" stroke={LEVEL_COLOURS[entry.role]} strokeWidth={2} />
              {entry.points.length === 1 || entry.points.some((point, index) => index > 0 && point.year - entry.points[index - 1]!.year > 1)
                ? entry.points.map((point) => <circle key={point.year} cx={geometry.x(point.year)} cy={geometry.y(point.value)} r={4} fill={LEVEL_COLOURS[entry.role]} stroke="var(--card)" strokeWidth={2} />)
                : null}
              <text x={geometry.x(last.year) + 8} y={labelYs[index]} className="public-money-end-label">
                {`${names.get(entry.geoId) || entry.geoId} ${percentText(last.value)}`}
              </text>
            </g>
          );
        })}
      </svg>
      <details className="public-money-table">
        <summary>Show as a table</summary>
        <table>
          <thead>
            <tr>
              <th scope="col">Year</th>
              {series.map((entry) => <th key={entry.geoId} scope="col">{names.get(entry.geoId) || entry.geoId}</th>)}
            </tr>
          </thead>
          <tbody>
            {years.map((year) => (
              <tr key={year}>
                <th scope="row">{year}</th>
                {series.map((entry) => <td key={entry.geoId}>{percentText(entry.points.find((point) => point.year === year)?.value ?? null)}</td>)}
              </tr>
            ))}
          </tbody>
        </table>
      </details>
    </figure>
  );
}

function Group({ title, testId, children }: { title: string; testId: string; children: React.ReactNode }) {
  return (
    <div className="public-money-group" data-testid={testId}>
      <h4>{title}</h4>
      {children}
    </div>
  );
}

export default function PublicMoney({ geoId, names }: { geoId: string; names: Record<string, string> }) {
  const [load, setLoad] = useState<Load>({ state: "loading" });
  const served = PUBLIC_MONEY_GEO_ID.test(geoId);
  const nameMap = useMemo(() => new Map(Object.entries(names)), [names]);

  useEffect(() => {
    // Anything but the nation, a state or a county is the API's 422: never asked.
    if (!served) return;
    let cancelled = false;
    setLoad({ state: "loading" });
    apiFetch<PublicMoneyAnswer>("/public-money", { params: { geo_id: geoId } })
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
  if (load.state === "loading") return <p className="subtle" data-testid="public-money-loading">Reading taxes and public money…</p>;
  if (load.state === "error") return <p className="subtle" data-testid="public-money-error">Taxes and public money could not be read: {load.message}</p>;
  const answer = load.answer;
  const placeName = nameMap.get(geoId) || geoId;
  const payments = answer ? paymentRows(answer) : [];
  const income = answer ? incomeRows(answer) : [];
  const property = answer ? propertyRow(answer) : null;
  const levels = answer?.levels || [];
  const taxRate = answer ? lineSeries(answer.federal_tax_rate, levels) : [];
  // State and local taxes for the state and the nation; a county area's local
  // taxes are another measure and are said in words, never on the same line.
  const isCounty = (id: string) => id.includes("|county:");
  const burden = answer ? lineSeries(answer.tax_burden, levels, (id) => !isCounty(id)) : [];
  const countyBurden = answer ? lineSeries(answer.tax_burden, levels, isCounty)[0] : undefined;
  const paymentsYear = answer?.payments[0]?.year;
  const incomeYear = answer?.income_sources[0]?.year;
  const gap = payments[0] ? nationGapSentence(payments[0], placeName, "payments are") : null;

  return (
    <section className="public-money" aria-labelledby="public-money-heading" data-testid="public-money">
      <h3 id="public-money-heading">Taxes and public money</h3>
      <p className="subtle">What residents receive from government and what is collected in taxes, each against {levels.length > 1 ? "the state and the nation" : "its own history"}. Each measure stands on its own; none is a score.</p>
      {levels.length > 1 ? <Legend levels={levels} names={nameMap} /> : null}

      <Group title={`Payments from government and others, share of personal income${paymentsYear ? `, ${paymentsYear}` : ""}`} testId="public-money-payments">
        {payments.length ? (
          <>
            <Strips rows={payments} names={nameMap} testId="public-money-payments-strips" caption="Payments by program as a share of personal income" />
            {gap ? <p className="public-money-sentence" data-testid="public-money-payments-gap">{gap}</p> : null}
            <p className="place-basis">{answer!.payments_source}</p>
            <p className="subtle">Payments respond to a place&apos;s age and income; a high share describes who lives there, it does not cause it.</p>
            {isCounty(geoId) ? (
              <p><Link className="text-link" href={PAYMENTS_MAP} data-testid="public-money-payments-map">See this share for every county on a map <ArrowRight size={13} /></Link></p>
            ) : null}
          </>
        ) : (
          <p className="subtle" data-testid="public-money-payments-empty">{NOT_LOADED}</p>
        )}
      </Group>

      <Group title={`Where income comes from${incomeYear ? `, ${incomeYear}` : ""}`} testId="public-money-income">
        {income.length ? (
          <>
            <Strips rows={income} names={nameMap} testId="public-money-income-strips" caption="Shares of personal income by source" />
            <p className="place-basis">BEA CAINC4 lines 46 and 47 over CAINC1 personal income; earnings are the rest</p>
          </>
        ) : (
          <p className="subtle" data-testid="public-money-income-empty">{NOT_LOADED}</p>
        )}
      </Group>

      <Group title="Federal income tax as a share of adjusted gross income" testId="public-money-tax-rate">
        {taxRate.length ? (
          <>
            <LevelLines series={taxRate} names={nameMap} testId="public-money-tax-rate-lines" label={`Federal income tax rate, ${placeName} and the nation`} />
            <p className="place-basis">{answer!.federal_tax_rate.source}</p>
            {answer!.federal_tax_rate.caveats.slice(1).map((caveat) => <p key={caveat} className="subtle">{caveat}</p>)}
            {isCounty(geoId) ? (
              <p><Link className="text-link" href={TAX_RATE_MAP} data-testid="public-money-tax-rate-map">See the federal tax rate for every county on a map <ArrowRight size={13} /></Link></p>
            ) : null}
          </>
        ) : (
          <p className="subtle" data-testid="public-money-tax-rate-empty">{NOT_LOADED}</p>
        )}
      </Group>

      <Group title="State and local taxes collected, share of personal income" testId="public-money-tax-burden">
        {burden.length ? (
          <>
            <LevelLines series={burden} names={nameMap} testId="public-money-tax-burden-lines" label="State and local taxes collected as a share of personal income" />
            <p className="place-basis">{answer!.tax_burden.source}</p>
            <p className="subtle">Fiscal years are shown as the calendar year they end in; 46 states end theirs on June 30. Where a tax is collected is not who bears it.</p>
          </>
        ) : (
          <p className="subtle" data-testid="public-money-tax-burden-empty">{NOT_LOADED}</p>
        )}
        {countyBurden ? (
          <p className="public-money-sentence" data-testid="public-money-county-burden">
            Local governments in the {placeName} area collected{" "}
            {countyBurden.points.map((point) => `${percentText(point.value)} of residents' income in fiscal ${point.year}`).join(", ")}.{" "}
            <span className="subtle">Census counts every local government only in years ending in 2 and 7; a government spanning counties is counted in its home county.</span>
          </p>
        ) : null}
      </Group>

      <Group title={`Property tax load${answer?.property_tax.period ? `, ${answer.property_tax.period} survey` : ""}`} testId="public-money-property">
        {property ? (
          <>
            <Strips rows={[property]} names={nameMap} testId="public-money-property-strips" caption="Median property tax as a percent of median home value" />
            <p className="place-basis">{answer!.property_tax.source}</p>
            <p className="subtle">{answer!.property_tax.caveats.join(" ")}</p>
          </>
        ) : (
          <p className="subtle" data-testid="public-money-property-empty">{NOT_LOADED}</p>
        )}
      </Group>

      <details className="public-money-words" data-testid="public-money-words">
        <summary>Words used here</summary>
        <dl>
          {PUBLIC_MONEY_TERMS.map((entry) => (
            <div key={entry.term}>
              <dt>{entry.term}</dt>
              <dd>{entry.definition}</dd>
            </div>
          ))}
        </dl>
      </details>
    </section>
  );
}
