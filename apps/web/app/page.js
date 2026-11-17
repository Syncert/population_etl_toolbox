"use client";

import Link from "next/link";
import dynamic from "next/dynamic";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useRouter } from "next/navigation";
import { ArrowRight } from "lucide-react";
import PlaceSearch, { usePlaceCatalog } from "../components/PlaceSearch";
import { getSources } from "../lib/api/client";
import { connectedSourcesBand } from "../lib/catalog";
import { FEATURED_PLACE } from "../lib/featuredPlace";
import { featurePlaceHref } from "../lib/placeDirectory";
import { discoverTileMetadata } from "../lib/tiles";

const ChoroplethMap = dynamic(() => import("../components/ChoroplethMap"), { ssr: false });

export default function HomePage() {
  const [sources, setSources] = useState([]);
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
    getSources().then((sourceItems) => {
      if (!cancelled) {
        setSources(sourceItems);
        setStatus("ready");
      }
    }).catch(() => {
      if (!cancelled) setStatus("error");
    });
    return () => { cancelled = true; };
  }, []);

  const sourceBand = useMemo(() => connectedSourcesBand(status, sources), [status, sources]);

  return (
    <main className="page-shell home-page">
      <section className="home-intro home-finder">
        <div className="section-kicker">Economic Data Studio</div>
        <h1>What is going on in your place?</h1>
        <p>One page for every city, county and state, and the nation. Each number sits beside its state&apos;s and the nation&apos;s, with its period and source.</p>
        <PlaceSearch catalog={catalog} error={placeError} />
      </section>

      <section className="home-map" aria-labelledby="home-map-heading">
        <h2 id="home-map-heading">Or choose a county on the map</h2>
        {tileStatus === "ready" ? (
          <ChoroplethMap
            rows={[]}
            tileMetadata={tileMetadata}
            geoLevel="COUNTY"
            legendTitle="Select a county to open its page"
            missingLabel="County"
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
        <h2 id="home-tools-heading">Tools for analysts</h2>
        <p>Every published measure, with maps, tables and downloads, each tied back to its source.</p>
        <div className="command-row">
          <Link className="button secondary" href="/explore">Open the explorer <ArrowRight size={16} /></Link>
          <Link className="button secondary" href="/catalog">Browse the catalog</Link>
          <Link className="button secondary" href="/use-cases">Start from a question</Link>
        </div>
      </section>

      {/* Always present so the first render is the baseline; a failure that
          arrives afterwards is announced rather than only shown (WEB-037). */}
      <div className="status-row" role="status" data-testid="home-status">
        {status === "error" ? (
          <div className="notice error">Live catalog data is temporarily unavailable.</div>
        ) : null}
      </div>

      <section className="source-band" aria-labelledby="home-sources-heading">
        <h2 id="home-sources-heading" data-testid="home-source-count">
          {sourceBand.names.length > 0 ? `${sourceBand.names.length} public sources, each named beside its numbers` : "Public sources, each named beside its numbers"}
        </h2>
        <ul className="source-list" data-testid="home-source-list">
          {/* Named only where the API named them: a list from this page
              would read as a fact about the warehouse (WEB-080). */}
          {sourceBand.names.length > 0 ? (
            sourceBand.names.map((name) => <li key={name}>{name}</li>)
          ) : (
            <li className="subtle">{sourceBand.message}</li>
          )}
        </ul>
        <p><Link className="text-link" href="/data">Where the numbers come from <ArrowRight size={13} /></Link></p>
      </section>
    </main>
  );
}
