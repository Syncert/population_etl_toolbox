// What `/map/<code>` says for a single B01001 age line (ADR-0013): not a
// map, and where the same numbers are read instead. Rendered by the route
// before the map page mounts, so it makes no request.

import Link from "next/link";

import { explorerHref } from "../lib/urlState";

export default function AgeLineNotMapped({ metricCode }: { metricCode: string }) {
  return (
    <main className="page-shell compact-page measure-map-page" data-testid="measure-map-age-line" data-metric={metricCode}>
      <header className="page-heading">
        <div className="section-kicker">Map</div>
        <h1>One age group is not drawn as a map</h1>
        <p>
          This measure is one sex in one of the Census Bureau&apos;s 23 age groups, which are of unequal width: some cover one
          year, others three or ten. A map of one of them is easily read beside another as if they were alike, so each place
          page draws them together instead, grouped into equal five-year bands as an age pyramid with its comparison.
        </p>
      </header>
      <ul className="place-index">
        <li><Link className="text-link" href="/us">Find a place and read its age pyramid</Link></li>
        <li><Link className="text-link" href={explorerHref({ metric: metricCode, source: "CENSUS_ACS" })} data-testid="measure-map-age-line-explore">Read this age group in the explorer</Link></li>
      </ul>
    </main>
  );
}
