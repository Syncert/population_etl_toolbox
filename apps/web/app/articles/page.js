"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { ArrowRight, CalendarDays } from "lucide-react";
import MiniLineChart from "../../components/MiniLineChart";
import SourceNote from "../../components/SourceNote";
import { getTimeseries } from "../../lib/api/client";
import { explorerHref } from "../../lib/urlState";

const metricCode = "ACS:acs5:B01003_001";
const exampleGeo = "state:55|county:025";
const exampleExplorerHref = explorerHref({
  metric: metricCode,
  geoId: exampleGeo,
  stateFips: "55",
});

export default function ArticlesPage() {
  const [history, setHistory] = useState([]);
  useEffect(() => {
    getTimeseries({ metric_code: metricCode, geo_id: exampleGeo, limit: "1000" })
      .then((payload) => setHistory(payload.items || []))
      .catch(() => setHistory([]));
  }, []);

  const first = history[0];
  const latest = history.at(-1);
  const growth = first && latest && Number(first.value) ? (Number(latest.value) / Number(first.value) - 1) * 100 : null;

  return (
    <main className="article-shell">
      <article>
        <header className="article-header"><div className="section-kicker">Population analysis</div><h1>Reading county population change without losing the source</h1><p className="article-deck">A live example of how narrative, reusable charts, and methodology can coexist in one public analytical page.</p><div className="article-byline"><span>Economic Data Studio</span><span><CalendarDays size={14} /> Live data</span><span>Source: Census ACS</span></div></header>
        <div className="article-copy"><p className="lede">County totals are easy to map and surprisingly easy to misread. Large places dominate a raw scale, while small differences may sit inside survey uncertainty.</p><p>The Studio keeps those analytical constraints beside the visual. The chart below is not a screenshot: it uses the same observation contract as the explorer and can be reopened with its metric and geography intact.</p></div>
        <section className="embedded-analysis">
          <div className="panel-heading"><div><div className="section-kicker">Live chart</div><h2>Dane County population</h2></div><strong>{growth == null ? "-" : `${growth >= 0 ? "+" : ""}${growth.toFixed(1)}%`}</strong></div>
          <MiniLineChart items={history} label="Dane County population" />
          <div className="embed-actions"><Link className="button secondary" href={exampleExplorerHref}>Open in Explorer <ArrowRight size={15} /></Link><button className="button ghost" type="button" onClick={() => navigator.clipboard?.writeText(`${window.location.origin}${exampleExplorerHref}`)}>Copy chart link</button></div>
        </section>
        <div className="article-copy"><h2>Interpretation needs distribution context</h2><p>A county map should expose its binning method, missing observations, selected geography, period, and margin of error. The explorer therefore obtains distribution bins from the API instead of inventing a color scale in isolation.</p><blockquote>Source notes are part of the product, not an appendix added after the analysis is finished.</blockquote></div>
        <SourceNote source="U.S. Census Bureau" dataset="ACS 5-year" metric="B01003_001 - Total population" geography="Dane County, Wisconsin" period={first && latest ? `${first.period || first.observation_date} to ${latest.period || latest.observation_date}` : "Latest available"} updatedAt={latest?.updated_at} caveats="Raw population is shown here as a time series. For national choropleths, use distribution-aware bins and inspect uncertainty before ranking close values." />
      </article>
    </main>
  );
}
