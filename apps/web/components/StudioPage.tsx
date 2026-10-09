"use client";

// The studio (studio-video-frames): an operator surface that renders one
// place-page headline measure as a video frame, exports it, and keeps a
// record of exactly which requests drew it. It needs a bearer credential;
// without one it shows the sign-in control and no frame. It reads, and never
// writes, anything but the frame records the operator saves.

import { useCallback, useEffect, useMemo, useState } from "react";
import { Download, Save } from "lucide-react";
import SignInControl from "./SignInControl";
import StatusPill from "./StatusPill";
import {
  apiErrorMessage,
  buildApiPath,
  createSavedAnalysis,
  fetchAllPages,
  getCapabilities,
  getMetric,
  getSavedAnalysis,
  listSavedAnalyses,
} from "../lib/api/client";
import type { CollectionResponse, GeographySummary, MetricSummary, Observation, SavedAnalysisSummary } from "../lib/api/types";
import { readStoredToken, subscribeToCredential } from "../lib/apiToken";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, marginOfErrorText, observationUnit, publishedNumber } from "../lib/explorerViewModel";
import { displayMetricName } from "../lib/format";
import {
  ACTIVE_GEOGRAPHIES_ONLY,
  buildNewestValueRequest,
  normalizeObservationRows,
  observationPeriodLabel,
  observationUncertaintyLabel,
  OBSERVATION_UNCERTAINTY_BEYOND_MARGIN,
} from "../lib/observationAccess";
import { observationExport } from "../lib/observationExport";
import { PLACE_CHAPTERS, countyName, countySegment, placePath, stateName, stateSegment } from "../lib/placeChapters";
import {
  FRAME_FORMATS,
  REVIEWED_DEFINITIONS,
  frameRecord,
  renderFrameSvg,
  scriptNotes,
  studioRecord,
} from "../lib/studioFrame";
import type { FrameFormat, FrameRow, FrameSpec, FrameTheme } from "../lib/studioFrame";

const FRAME_PREFIX = "Frame · ";

interface Drawn {
  rows: { level: FrameRow["level"]; geoId: string; name: string; row: ObservationRow | null; url: string }[];
}

function download(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 10_000);
}

/** Rasterise the frame's own SVG at its declared size: the same drawing, not a screenshot. */
async function svgToPng(svg: string, width: number, height: number): Promise<Blob> {
  const url = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml" }));
  try {
    const image = new Image(width, height);
    await new Promise<void>((resolve, reject) => {
      image.onload = () => resolve();
      image.onerror = () => reject(new Error("the frame could not be drawn"));
      image.src = url;
    });
    const canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    canvas.getContext("2d")!.drawImage(image, 0, 0, width, height);
    return await new Promise<Blob>((resolve, reject) =>
      canvas.toBlob((blob) => (blob ? resolve(blob) : reject(new Error("the PNG could not be encoded"))), "image/png"),
    );
  } finally {
    URL.revokeObjectURL(url);
  }
}

function uncertaintyOf(row: ObservationRow | null): string {
  if (!row) return "";
  const margin = marginOfErrorText(row);
  return [margin !== "Not provided" ? `margin of error ${margin}` : "", observationUncertaintyLabel(row, OBSERVATION_UNCERTAINTY_BEYOND_MARGIN)]
    .filter(Boolean)
    .join(", ");
}

export default function StudioPage() {
  const [token, setToken] = useState("");
  // A record to reopen, from the address (`/studio?record=<id>`). Read on the
  // client, so the route renders without awaiting anything on the server.
  const [recordId, setRecordId] = useState<number | null>(null);
  useEffect(() => {
    const value = Number(new URLSearchParams(window.location.search).get("record"));
    setRecordId(Number.isInteger(value) && value > 0 ? value : null);
  }, []);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [nation, setNation] = useState<GeographySummary | null>(null);
  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [stateFips, setStateFips] = useState("55");
  const [countyId, setCountyId] = useState("state:55|county:025");
  const [chapterId, setChapterId] = useState(PLACE_CHAPTERS[0]!.id);
  const [measureId, setMeasureId] = useState(PLACE_CHAPTERS[0]!.headline[0]!.id);
  const [format, setFormat] = useState<FrameFormat>("16:9");
  const [theme, setTheme] = useState<FrameTheme>("light");
  const [metric, setMetric] = useState<MetricSummary | null>(null);
  const [drawn, setDrawn] = useState<Drawn | null>(null);
  const [status, setStatus] = useState({ state: "idle", message: "choose a frame" });
  const [history, setHistory] = useState<SavedAnalysisSummary[]>([]);
  const [replayed, setReplayed] = useState<{ urls: string[] } | null>(null);
  const [notice, setNotice] = useState("");

  useEffect(() => {
    const sync = () => setToken(readStoredToken());
    sync();
    return subscribeToCredential(sync);
  }, []);

  const chapter = PLACE_CHAPTERS.find((item) => item.id === chapterId) || PLACE_CHAPTERS[0]!;
  const measure = chapter.headline.find((item) => item.id === measureId) || chapter.headline[0]!;
  const county = counties.find((item) => item.geo_id === countyId) || null;
  const state = states.find((item) => item.state_fips === stateFips) || null;

  // The catalog the picker offers.
  useEffect(() => {
    if (!token) return;
    const controller = new AbortController();
    const read = (geo_level: string, extra: Record<string, string> = {}) =>
      fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level, ...extra }, pageSize: 1000, signal: controller.signal });
    Promise.all([read("NATIONAL"), read("STATE"), getCapabilities({ signal: controller.signal })])
      .then(([nations, stateItems, capabilities]) => {
        if (controller.signal.aborted) return;
        setNation(nations[0] || null);
        setStates([...stateItems].sort((left, right) => stateName(left).localeCompare(stateName(right))));
        setSources(buildExplorerSources(capabilities.items));
      })
      .catch((error) => setStatus({ state: "bad", message: apiErrorMessage(error) }));
    return () => controller.abort();
  }, [token]);

  useEffect(() => {
    if (!token || !stateFips) return;
    const controller = new AbortController();
    fetchAllPages<GeographySummary>("/catalog/geographies", { params: { ...ACTIVE_GEOGRAPHIES_ONLY, geo_level: "COUNTY", state_fips: stateFips }, pageSize: 1000, signal: controller.signal })
      .then((items) => {
        if (!controller.signal.aborted) setCounties(items.filter((item) => item.state_fips === stateFips).sort((left, right) => countyName(left).localeCompare(countyName(right))));
      })
      .catch(() => undefined);
    return () => controller.abort();
  }, [token, stateFips]);

  const refreshHistory = useCallback(() => {
    if (!token) return;
    listSavedAnalyses(token, { limit: 50 })
      .then((payload) => setHistory((payload.items || []).filter((item) => item.name.startsWith(FRAME_PREFIX))))
      .catch(() => setHistory([]));
  }, [token]);
  useEffect(refreshHistory, [refreshHistory]);

  // Read the frame's three values, or replay a record's exact requests.
  useEffect(() => {
    if (!token || !sources.length || !nation || !state || !county) return;
    const controller = new AbortController();
    const signal = controller.signal;
    setStatus({ state: "loading", message: "reading the frame's values" });
    (async () => {
      try {
        const found = await getMetric(measure.candidates[0]!, { signal });
        setMetric(found);
        const source = sources.find((item) => item.sourceCode === found.source_code) || null;
        if (!source) throw new Error(`no declared access shape for ${found.source_code}`);
        const levels = [
          { level: "COUNTY" as const, geoId: county.geo_id, name: countyName(county) },
          { level: "STATE" as const, geoId: state.geo_id, name: stateName(state) },
          { level: "NATIONAL" as const, geoId: nation.geo_id, name: "United States" },
        ];
        const rows = await Promise.all(
          levels.map(async (entry, index) => {
            const url = replayed?.urls[index] || (() => {
              const { resource, params } = buildNewestValueRequest(source, { metricCode: found.metric_code, geoId: entry.geoId });
              return buildApiPath(resource, params);
            })();
            const response = await fetch(url, { signal, cache: "default" });
            if (!response.ok) return { ...entry, row: null, url };
            const payload = (await response.json()) as CollectionResponse<Observation>;
            const normalized = normalizeObservationRows(source, payload.items || []).filter((row) => row.geo_id === entry.geoId && row.metric_code === found.metric_code);
            return { ...entry, row: normalized.at(-1) ?? null, url };
          }),
        );
        if (signal.aborted) return;
        setDrawn({ rows });
        setStatus({ state: "ok", message: replayed ? "re-rendered from the record's requests" : "frame drawn from the published values" });
      } catch (error) {
        if (!signal.aborted) setStatus({ state: "bad", message: apiErrorMessage(error) });
      }
    })();
    return () => controller.abort();
  }, [token, sources, nation, state, county, measure, replayed]);

  // Reopen a stored record.
  useEffect(() => {
    if (!token || !recordId) return;
    getSavedAnalysis(token, recordId)
      .then((configuration) => {
        const record = studioRecord(configuration.document);
        if (!record) {
          setNotice("That saved item is not a studio frame.");
          return;
        }
        const foundChapter = PLACE_CHAPTERS.find((item) => item.id === record.chapterId);
        setFormat(record.format);
        setTheme(record.theme);
        if (foundChapter) setChapterId(foundChapter.id);
        setMeasureId(record.measureId);
        const countyRequest = record.requests.find((request) => request.level === "COUNTY");
        if (countyRequest) {
          setStateFips(countyRequest.geoId.slice(6, 8));
          setCountyId(countyRequest.geoId);
        }
        setReplayed({ urls: ["COUNTY", "STATE", "NATIONAL"].map((level) => record.requests.find((request) => request.level === level)?.url || "") });
        setNotice(`Reopened ${configuration.name}.`);
      })
      .catch((error) => setNotice(apiErrorMessage(error)));
  }, [token, recordId]);

  const spec: FrameSpec | null = useMemo(() => {
    if (!drawn || !metric) return null;
    const period = drawn.rows.map((entry) => (entry.row ? observationPeriodLabel(entry.row) : "")).find(Boolean) || "";
    const unitRow = drawn.rows.find((entry) => entry.row)?.row;
    const unit = unitRow && observationUnit(unitRow) !== "value" ? observationUnit(unitRow) : "";
    return {
      format,
      theme,
      title: `${measure.label}: ${county ? countyName(county) : ""}, ${state ? stateName(state) : ""}`,
      metricCode: metric.metric_code,
      measureName: displayMetricName(metric),
      unit,
      period,
      source: String(metric.source_code || ""),
      caveat: chapter.caveat.split(". ")[0] || "",
      rows: drawn.rows.map((entry) => {
        const own = entry.row && observationPeriodLabel(entry.row) === period ? entry.row : null;
        const number = own ? publishedNumber(own.value) : null;
        return {
          level: entry.level,
          name: entry.name,
          value: number,
          valueText: own && number !== null ? `${formatObservationValue(own.value)}${unit ? ` ${unit}` : ""}` : entry.row ? `not published for ${period}` : "not published",
          uncertainty: uncertaintyOf(own),
        };
      }),
    };
  }, [drawn, metric, format, theme, measure, county, state, chapter]);

  const svg = useMemo(() => (spec ? renderFrameSvg(spec) : ""), [spec]);
  const preview = useMemo(() => (svg ? URL.createObjectURL(new Blob([svg], { type: "image/svg+xml" })) : ""), [svg]);
  useEffect(() => () => { if (preview) URL.revokeObjectURL(preview); }, [preview]);

  const record = useMemo(() => {
    if (!spec || !drawn || !county || !state) return null;
    return frameRecord({
      spec,
      placePath: placePath(stateSegment(state, states), countySegment(county, counties)),
      chapterId: chapter.id,
      measureId: measure.id,
      requests: drawn.rows.map((entry) => ({ level: entry.level, geoId: entry.geoId, url: entry.url })),
      releases: Object.fromEntries(drawn.rows.filter((entry) => entry.row?.release).map((entry) => [entry.geoId, String(entry.row!.release)])),
      newestPerGeography: Boolean(sources.find((item) => item.sourceCode === metric?.source_code)?.supportsNewestPerGeography),
    });
  }, [spec, drawn, county, state, states, counties, chapter, measure, sources, metric]);

  const notes = metric
    ? scriptNotes({
        harvestedLabel: displayMetricName(metric),
        definition: REVIEWED_DEFINITIONS[metric.metric_code] || null,
        caveats: [chapter.caveat, measure.note || ""],
        period: spec?.period || "",
      })
    : null;

  if (!token) {
    return (
      <main className="page-shell compact-page" data-testid="studio-signed-out">
        <header className="page-heading">
          <div className="section-kicker">Studio</div>
          <h1>Sign in to use the studio</h1>
          <p>The studio is for operators preparing video frames. It needs a signed-in session or an operator token.</p>
        </header>
        <SignInControl />
      </main>
    );
  }

  const size = FRAME_FORMATS[format];
  return (
    <main className="page-shell studio-page" data-testid="studio">
      <header className="page-heading">
        <div className="section-kicker">Studio</div>
        <h1>Video frames from the place pages</h1>
        <p>Every frame carries its measure, period, source, and uncertainty in the image itself.</p>
      </header>
      <section className="profile-controls studio-controls">
        <label>State<select value={stateFips} onChange={(event) => { setReplayed(null); setStateFips(event.target.value); setCountyId(""); }} data-testid="studio-state">{states.map((item) => <option key={item.geo_id} value={item.state_fips || ""}>{stateName(item)}</option>)}</select></label>
        <label>County<select value={countyId} onChange={(event) => { setReplayed(null); setCountyId(event.target.value); }} data-testid="studio-county"><option value="">Choose a county</option>{counties.map((item) => <option key={item.geo_id} value={item.geo_id}>{countyName(item)}</option>)}</select></label>
        <label>Chapter<select value={chapterId} onChange={(event) => { setReplayed(null); const next = PLACE_CHAPTERS.find((item) => item.id === event.target.value)!; setChapterId(next.id); setMeasureId(next.headline[0]!.id); }} data-testid="studio-chapter">{PLACE_CHAPTERS.map((item) => <option key={item.id} value={item.id}>{item.title}</option>)}</select></label>
        <label>Card<select value={measure.id} onChange={(event) => { setReplayed(null); setMeasureId(event.target.value); }} data-testid="studio-measure">{chapter.headline.map((item) => <option key={item.id} value={item.id}>{item.label}</option>)}</select></label>
        <label>Format<select value={format} onChange={(event) => setFormat(event.target.value as FrameFormat)} data-testid="studio-format">{Object.entries(FRAME_FORMATS).map(([key, value]) => <option key={key} value={key}>{key} ({value.width}x{value.height})</option>)}</select></label>
        <label>Theme<select value={theme} onChange={(event) => setTheme(event.target.value as FrameTheme)} data-testid="studio-theme"><option value="light">Light</option><option value="dark">Dark</option></select></label>
      </section>
      <section className="status-row" role="status">
        <StatusPill state={status.state} label="Frame" message={status.message} testId="studio-status" />
      </section>
      {notice ? <p className="notice" role="status" data-testid="studio-notice">{notice}</p> : null}

      {spec && preview ? (
        <section className="analysis-panel" aria-labelledby="studio-frame-heading">
          <h2 id="studio-frame-heading">Frame, {size.width} by {size.height}</h2>
          {/* A blob: URL of the frame's own SVG: there is nothing for next/image to optimise or fetch. */}
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img className="studio-preview" src={preview} width={size.width} height={size.height} alt={`${spec.title}. ${spec.rows.map((row) => `${row.name}: ${row.valueText}`).join(". ")}.`} data-testid="studio-frame" data-format={format} />
          <div className="command-row">
            <button type="button" className="button primary" data-testid="studio-export-png" onClick={async () => {
              try { download(await svgToPng(svg, size.width, size.height), `frame-${spec.metricCode.replace(/[:|]/g, "-")}-${format.replace(":", "x")}.png`); } catch (error) { setNotice(apiErrorMessage(error)); }
            }}><Download size={15} /> PNG</button>
            <button type="button" className="button secondary" data-testid="studio-export-csv" onClick={() => {
              const exported = observationExport(drawn!.rows.flatMap((entry) => (entry.row ? [entry.row] : [])), { scope: "latest" });
              const csv = [exported.headings, ...exported.rows].map((row) => row.map((cell) => `"${String(cell ?? "").replaceAll('"', '""')}"`).join(",")).join("\n");
              download(new Blob([csv], { type: "text/csv;charset=utf-8" }), `frame-${spec.metricCode.replace(/[:|]/g, "-")}.csv`);
            }}><Download size={15} /> CSV</button>
            <button type="button" className="button secondary" data-testid="studio-export-record" onClick={() => download(new Blob([JSON.stringify(record, null, 2)], { type: "application/json" }), "frame-record.json")}><Download size={15} /> Frame record</button>
            <button type="button" className="button secondary" data-testid="studio-save" onClick={async () => {
              try {
                await createSavedAnalysis(token, { name: `${FRAME_PREFIX}${spec.title} · ${format} · ${new Date().toISOString().slice(0, 19)}`, document: record! });
                setNotice("Frame record saved to your account.");
                refreshHistory();
              } catch (error) { setNotice(apiErrorMessage(error)); }
            }}><Save size={15} /> Save record</button>
          </div>
        </section>
      ) : null}

      {notes ? (
        <section className="analysis-panel" aria-labelledby="studio-notes-heading" data-testid="studio-notes">
          <h2 id="studio-notes-heading">Script notes</h2>
          <p><strong>Definition ({notes.reviewState}):</strong> {notes.definition}</p>
          <p>{notes.period}</p>
          <ul>{notes.caveats.map((caveat) => <li key={caveat}>{caveat}</li>)}</ul>
          {spec ? <p className="subtle">Requests: {drawn?.rows.map((entry) => entry.url).join(" · ")}</p> : null}
        </section>
      ) : null}

      <section className="analysis-panel" aria-labelledby="studio-history-heading" data-testid="studio-history">
        <h2 id="studio-history-heading">Saved frame records</h2>
        {history.length ? (
          <ul className="place-index">{history.map((item) => <li key={item.configuration_id}><a href={`/studio?record=${item.configuration_id}`}>{item.name}</a></li>)}</ul>
        ) : (
          <p className="subtle">No frame records saved yet.</p>
        )}
      </section>
    </main>
  );
}
