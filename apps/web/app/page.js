"use client";

import Link from "next/link";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowRight, BarChart3, BookOpen, Database, Map } from "lucide-react";
import PlaceSearch, { usePlaceCatalog } from "../components/PlaceSearch";
import { getSources, searchMetrics } from "../lib/api/client";
import { displayMetricName, formatNumber } from "../lib/format";
import { connectedSourcesBand } from "../lib/catalog";
import { FEATURED_PLACE } from "../lib/featuredPlace";
import { featurePlaceHref } from "../lib/placeDirectory";
import { discoverTileMetadata } from "../lib/tiles";
import { explorerHref } from "../lib/urlState";

const ChoroplethMap = dynamic(() => import("../components/ChoroplethMap"), { ssr: false });

export default function HomePage() {
  const [sources, setSources] = useState([]);
  const [metrics, setMetrics] = useState({ total: 0, items: [] });
  const [status, setStatus] = useState("loading");
  const router = useRouter();
  const { catalog, error: placeError } = usePlaceCatalog();
  const [tileMetadata, setTileMetadata] = useState(null);
  const [tileStatus, setTileStatus] = useState("loading");

  useEffect(() => {
    let cancelled = false;
    discoverTileMetadata()
      .then((metadata) => { if (!cancelled) { setTileMetadata(metadata); setTileStatus("ready"); } })
      .catch(() => { if (!cancelled) setTileStatus("unavailable"); });
    return () => { cancelled = true; };
  }, []);

  const openFeature = useCallback((properties) => {
    if (!catalog || !tileMetadata) return;
    const href = featurePlaceHref(properties, tileMetadata.joinKey || "geo_id", catalog.counties, catalog.entries);
    if (href) router.push(href);
  }, [catalog, tileMetadata, router]);

  const featured = useMemo(() => {
    if (!catalog) return [];
    const county = catalog.entries.find((entry) => entry.geoId === FEATURED_PLACE.countyGeoId);
    const state = catalog.entries.find((entry) => entry.geoId === FEATURED_PLACE.stateGeoId);
    const nation = catalog.entries.find((entry) => entry.level === "NATIONAL");
    return [county, state, nation].filter(Boolean);
  }, [catalog]);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      getSources(),
      searchMetrics({ active_only: "true", q: "population", limit: "6" }),
    ]).then(([sourceItems, metricPayload]) => {
      if (!cancelled) {
        setSources(sourceItems);
        setMetrics(metricPayload);
        setStatus("ready");
      }
    }).catch(() => {
      if (!cancelled) setStatus("error");
    });
    return () => { cancelled = true; };
  }, []);

  // The first metric the catalog answers for this search. It used to prefer
  // one hard-coded Census variable, which made the home page's feature a
  // client-authored choice of provider dressed as the catalog's answer -- and
  // pointed the explorer at that code even when the catalog had never
  // published it.
  const featuredMetric = useMemo(() => metrics.items[0], [metrics]);
  const sourceBand = useMemo(() => connectedSourcesBand(status, sources), [status, sources]);

  return (
    <main className="page-shell home-page">
      <section className="home-intro home-finder">
        <div className="section-kicker">Economic Data Studio</div>
        <h1>What is going on in your place?</h1>
        <p>One page for every county, every state, and the nation: the same chapters in the same order, each number beside its state&apos;s and the nation&apos;s, with its period and source.</p>
        <PlaceSearch catalog={catalog} error={placeError} />
      </section>

      <section className="home-map" aria-labelledby="home-map-heading">
        <h2 id="home-map-heading">Or choose a county on the map</h2>
        {tileStatus === "ready" ? (
          <ChoroplethMap
            rows={[]}
            tileMetadata={tileMetadata}
            geoLevel="COUNTY"
            legendTitle="Counties, unpainted: no briefing measure is published yet"
            missingLabel="Not painted"
            testId="home-map"
            ariaLabel="County map. Select a county to open its page; the search above reaches every county too."
            onFeatureClick={openFeature}
          />
        ) : (
          <p className="subtle" role="status" data-testid="home-map-status">
            {tileStatus === "loading" ? "Loading the county map…" : "The county map is not available here. The search above reaches every county."}
          </p>
        )}
      </section>

      {featured.length ? (
        <section className="home-featured" aria-labelledby="home-featured-heading">
          <h2 id="home-featured-heading">Start with an example</h2>
          <ul className="place-index" data-testid="home-featured-places">
            {featured.map((entry) => <li key={entry.geoId}><Link href={entry.href}>{entry.name}</Link></li>)}
          </ul>
        </section>
      ) : null}

      <section className="home-tools" aria-labelledby="home-tools-heading">
        <div className="section-kicker">Tools</div>
        <h2 id="home-tools-heading">For analysts</h2>
        <p>Explore published federal statistics with every metric, map, and chart tied back to its source.</p>
        <div className="command-row">
          <Link className="button primary" href="/explore">Open the explorer <ArrowRight size={16} /></Link>
          <Link className="button secondary" href="/catalog">Browse the catalog</Link>
          <Link className="button secondary" href="/data">Where the numbers come from</Link>
        </div>
      </section>

      <section className="signal-strip" aria-label="Platform signals">
        <div><strong>{status === "ready" ? sources.length : "-"}</strong><span>connected sources</span></div>
        <div><strong>{status === "ready" ? formatNumber(metrics.total) : "-"}</strong><span>population matches</span></div>
        {/* Two further cells stood here: "County / national map coverage" and
            "Live / API-backed observations". Neither came from anything the
            API answers -- the first is a claim about which grains the
            warehouse publishes, which varies by source, and the second is a
            claim about the deployment's own health that this page does not
            check. The two that remain are counts the catalog just gave us. */}
      </section>

      {/* Always present so the first render is the baseline; a failure that
          arrives afterwards is announced rather than only shown (WEB-037). */}
      <div className="status-row" role="status" data-testid="home-status">
        {status === "error" ? (
          <div className="notice error">Live catalog data is temporarily unavailable.</div>
        ) : null}
      </div>

      <section className="home-grid">
        <article className="feature-story">
          <div className="section-kicker">Featured analysis</div>
          <h2>Population concentration is best read county by county</h2>
          <p>Use the national county explorer to see the latest estimate, inspect uncertainty, and pin any county for its historical series.</p>
          <Link className="text-link" href="/articles">Read the analysis <BookOpen size={15} /></Link>
        </article>
        <article className="snapshot-panel">
          <div className="snapshot-icon"><Map aria-hidden="true" /></div>
          <div>
            <div className="section-kicker">National snapshot</div>
            <h2>{featuredMetric ? displayMetricName(featuredMetric) : "County population estimates"}</h2>
            <p>Latest source-backed metric metadata and observation coverage.</p>
            {/* Only where the catalog answered one. A link built from a
                metric code this client invented opens the explorer on a
                measure the API may never have published. */}
            {featuredMetric ? (
              <Link
                className="text-link"
                data-testid="home-featured-link"
                href={explorerHref({
                  metric: featuredMetric.metric_code,
                  source: featuredMetric.source_code || undefined,
                })}
              >
                Open map <ArrowRight size={15} />
              </Link>
            ) : (
              <Link className="text-link" data-testid="home-featured-link" href="/catalog">
                Browse the catalog <ArrowRight size={15} />
              </Link>
            )}
          </div>
        </article>
      </section>

      <section className="path-grid" aria-label="Primary workflows">
        <Link href="/catalog"><Database /><strong>Catalog</strong><span>Find metrics and inspect provenance.</span></Link>
        <Link href="/explore"><BarChart3 /><strong>Explore</strong><span>Map, compare, and save a view.</span></Link>
        <Link href="/builder"><BookOpen /><strong>Compose</strong><span>Build a page from reusable analysis.</span></Link>
        <Link href="/use-cases"><Map /><strong>Use cases</strong><span>Start with a reviewed question and choose a place.</span></Link>
      </section>

      <section className="source-band">
        <div><div className="section-kicker">Connected sources</div><h2>Public data with its identity intact</h2></div>
        <div className="source-list" data-testid="home-source-list">
          {/* Named only where the API named them: a list from this page
              would read as a fact about the warehouse (WEB-080). */}
          {sourceBand.names.length > 0 ? (
            sourceBand.names.map((name) => <span key={name}>{name}</span>)
          ) : (
            <span className="subtle">{sourceBand.message}</span>
          )}
        </div>
      </section>
    </main>
  );
}
