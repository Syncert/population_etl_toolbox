"use client";

// A first-wave product screen: one place, read through a product template.
//
// The screen is generic. Which measures appear, in what order, under what
// headings, is entirely `lib/productTemplates` configuration; this component
// resolves those slots against the published catalog, asks each resolved
// measure for the selected place, and renders every answer with its own
// source, period, unit, uncertainty, and quality state.
//
// It computes nothing. There is no score, no index, no ranking, and no
// cross-measure arithmetic — each measure stands on its own, and a slot the
// catalog cannot fill leaves a stated gap rather than disappearing.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import { ArrowRight, Download, Save } from "lucide-react";
import StatusPill from "./StatusPill";
import {
  ApiError,
  apiErrorMessage,
  apiFetch,
  fetchAllPages,
  getCapabilities,
  getMetric,
} from "../lib/api/client";
import { createRequestTracker } from "../lib/api/requestState";
import type { CollectionResponse, GeographySummary, MetricSummary, Observation } from "../lib/api/types";
import { metricQualityState } from "../lib/catalog";
import { buildExplorerSources } from "../lib/explorerSources";
import type { ExplorerSource } from "../lib/explorerSources";
import {
  buildHistoryObservationRequest,
  normalizeObservationRows,
  observationPeriodLabel,
} from "../lib/observationAccess";
import type { ObservationRow } from "../lib/explorerViewModel";
import { formatObservationValue, marginOfErrorText, observationUnit } from "../lib/explorerViewModel";
import { displayMetricName } from "../lib/format";
import {
  DEFAULT_TEMPLATE_ID,
  PRODUCT_TEMPLATES,
  findTemplate,
  resolveTemplate,
  templateCoverage,
  templateMetricCodes,
} from "../lib/productTemplates";
import type { ResolvedMeasure } from "../lib/productTemplates";
import { saveChart } from "../lib/savedCharts";
import { explorerHref, parseProfileState, serializeProfileState } from "../lib/urlState";

const CATALOG_PAGE_SIZE = 1000;

interface RequestStatus {
  state: string;
  message: string;
}

interface MeasureAnswer {
  /** The published row for this place, or `null` when none was published. */
  row: ObservationRow | null;
  state: string;
  message: string;
}

export default function ProfileProduct() {
  const capabilitiesTracker = useRef(createRequestTracker()).current;
  const catalogTracker = useRef(createRequestTracker()).current;
  const geographyTracker = useRef(createRequestTracker()).current;
  const observationTracker = useRef(createRequestTracker()).current;
  const requestedRef = useRef<ReturnType<typeof parseProfileState> | null>(null);

  const [templateId, setTemplateId] = useState(DEFAULT_TEMPLATE_ID);
  const [sources, setSources] = useState<ExplorerSource[]>([]);
  const [states, setStates] = useState<GeographySummary[]>([]);
  const [counties, setCounties] = useState<GeographySummary[]>([]);
  const [stateFips, setStateFips] = useState("");
  const [geoId, setGeoId] = useState("");
  const [metricsByCode, setMetricsByCode] = useState<Map<string, MetricSummary>>(new Map());
  const [catalogStatus, setCatalogStatus] = useState<RequestStatus>({
    state: "loading",
    message: "resolving published measures",
  });
  const [answers, setAnswers] = useState<Record<string, MeasureAnswer>>({});
  const [observationStatus, setObservationStatus] = useState<RequestStatus>({
    state: "idle",
    message: "select a place",
  });
  const [saveStatus, setSaveStatus] = useState("");

  const template = useMemo(() => findTemplate(templateId) || PRODUCT_TEMPLATES[0]!, [templateId]);
  const resolved = useMemo(
    () => resolveTemplate(template, metricsByCode),
    [template, metricsByCode],
  );
  const coverage = useMemo(() => templateCoverage(resolved), [resolved]);
  const availableMeasures = useMemo(
    () => resolved.flatMap((entry) => entry.measures).filter((measure) => measure.available),
    [resolved],
  );

  const sourceForMetric = useCallback(
    (metric: MetricSummary | null): ExplorerSource | null =>
      sources.find((source) => source.sourceCode === metric?.source_code) || null,
    [sources],
  );

  useEffect(() => {
    const request = capabilitiesTracker.begin();
    requestedRef.current = parseProfileState(window.location.search);
    if (requestedRef.current.template) {
      setTemplateId(requestedRef.current.template);
    }
    if (requestedRef.current.geoId) {
      setGeoId(requestedRef.current.geoId);
    }

    (async () => {
      try {
        const payload = await getCapabilities();
        if (request.isCurrent()) {
          setSources(buildExplorerSources(payload.items));
        }
      } catch {
        // Access shapes are unavailable; each measure then reports that its
        // source's declared filters could not be read, rather than guessing.
      }
    })();
    return () => {
      capabilitiesTracker.invalidate();
    };
  }, [capabilitiesTracker]);

  useEffect(() => {
    const request = geographyTracker.begin();
    (async () => {
      try {
        const [stateItems, countyItems] = await Promise.all([
          fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { geo_level: "STATE" },
            pageSize: CATALOG_PAGE_SIZE,
          }),
          fetchAllPages<GeographySummary>("/catalog/geographies", {
            params: { geo_level: "COUNTY" },
            pageSize: CATALOG_PAGE_SIZE,
          }),
        ]);
        if (!request.isCurrent()) {
          return;
        }
        setStates(
          stateItems.sort((left, right) =>
            String(left.state_name).localeCompare(String(right.state_name)),
          ),
        );
        setCounties(
          countyItems.sort((left, right) =>
            String(left.county_name).localeCompare(String(right.county_name)),
          ),
        );
      } catch {
        // The place picker stays empty; the profile says it has no place.
      }
    })();
    return () => {
      geographyTracker.invalidate();
    };
  }, [geographyTracker]);

  // Resolve every candidate identity the template names against the
  // published catalog. A 404 is the API's stable answer for "not published",
  // and is recorded as an unfilled slot rather than an error.
  useEffect(() => {
    const request = catalogTracker.begin();
    const codes = templateMetricCodes(template);
    setCatalogStatus({ state: "loading", message: "resolving published measures" });

    (async () => {
      const found = new Map<string, MetricSummary>();
      let failures = 0;
      await Promise.all(
        codes.map(async (code) => {
          try {
            const metric = await getMetric(code);
            if (metric?.metric_code) {
              found.set(metric.metric_code, metric);
            }
          } catch (error) {
            if (!(error instanceof ApiError && error.status === 404)) {
              failures += 1;
            }
          }
        }),
      );
      if (!request.isCurrent()) {
        return;
      }
      setMetricsByCode(found);
      setCatalogStatus(
        failures > 0
          ? {
              state: "warn",
              message: `${found.size} of ${codes.length} identities resolved; ${failures} could not be checked`,
            }
          : {
              state: found.size === codes.length ? "ok" : "warn",
              message: `${found.size} of ${codes.length} candidate identities are published`,
            },
      );
    })();

    return () => {
      catalogTracker.invalidate();
    };
  }, [catalogTracker, template]);

  // One request per filled slot, for the selected place. Each goes through
  // the access shape its own source declares.
  useEffect(() => {
    if (!geoId || availableMeasures.length === 0) {
      setAnswers({});
      setObservationStatus({
        state: "idle",
        message: geoId ? "no published measure to ask for" : "select a place",
      });
      return;
    }

    const request = observationTracker.begin();
    setObservationStatus({ state: "loading", message: `asking ${availableMeasures.length} measures` });

    (async () => {
      const next: Record<string, MeasureAnswer> = {};
      await Promise.all(
        availableMeasures.map(async (measure) => {
          const source = sourceForMetric(measure.metric);
          if (!source) {
            next[measure.slot.id] = {
              row: null,
              state: "warn",
              message: `no declared access shape for source ${measure.metric?.source_code || "unknown"}`,
            };
            return;
          }
          try {
            const { resource, params } = buildHistoryObservationRequest(source, {
              metricCode: measure.metricCode,
              geoId,
              limit: "50",
            });
            const payload = await apiFetch<CollectionResponse<Observation>>(resource, { params });
            const rows = normalizeObservationRows(
              source,
              Array.isArray(payload.items) ? payload.items : [],
            );
            next[measure.slot.id] = rows.length
              ? { row: rows[rows.length - 1]!, state: "ok", message: `${rows.length} published` }
              : {
                  row: null,
                  state: "warn",
                  message: "not published for this place",
                };
          } catch (error) {
            next[measure.slot.id] = {
              row: null,
              state: "bad",
              message: apiErrorMessage(error),
            };
          }
        }),
      );
      if (!request.isCurrent()) {
        return;
      }
      setAnswers(next);
      const answered = Object.values(next).filter((answer) => answer.row).length;
      setObservationStatus({
        state: answered === availableMeasures.length ? "ok" : "warn",
        message: `${answered} of ${availableMeasures.length} measures published a value for this place`,
      });
    })();

    return () => {
      observationTracker.invalidate();
    };
  }, [observationTracker, geoId, availableMeasures, sourceForMetric]);

  const place = useMemo(
    () =>
      counties.find((item) => item.geo_id === geoId) ||
      states.find((item) => item.geo_id === geoId) ||
      null,
    [counties, states, geoId],
  );
  const placeName = place
    ? [place.county_name, place.state_name].filter(Boolean).join(", ") || String(place.geo_id)
    : "";
  const scopedCounties = useMemo(
    () => (stateFips ? counties.filter((item) => item.state_fips === stateFips) : counties),
    [counties, stateFips],
  );

  useEffect(() => {
    const query = serializeProfileState(
      { template: templateId, geoId },
      { template: DEFAULT_TEMPLATE_ID },
    );
    const nextUrl = query ? `${window.location.pathname}?${query}` : window.location.pathname;
    if (`${window.location.pathname}${window.location.search}` !== nextUrl) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [templateId, geoId]);

  function exportCsv() {
    const headings = [
      "product",
      "section",
      "slot",
      "metric_code",
      "metric_name",
      "source",
      "geo_id",
      "geo_name",
      "period",
      "value",
      "value_status",
      "unit",
      "margin_of_error",
      "availability",
    ];
    const rows: string[][] = [];
    for (const entry of resolved) {
      for (const measure of entry.measures) {
        const answer = answers[measure.slot.id];
        const row = answer?.row;
        rows.push([
          template.title,
          entry.section.title,
          measure.slot.label,
          measure.metricCode,
          measure.metric ? displayMetricName(measure.metric) : "",
          String(measure.metric?.source_code ?? ""),
          geoId,
          placeName,
          row ? observationPeriodLabel(row) : "",
          row?.value == null ? "" : String(row.value),
          String(row?.value_status ?? ""),
          row ? observationUnit(row) : "",
          row?.margin_of_error == null ? "" : String(row.margin_of_error),
          measure.available ? answer?.message || "not requested" : measure.reason,
        ]);
      }
    }
    const escape = (value: unknown) => `"${String(value ?? "").replaceAll('"', '""')}"`;
    const content = [headings, ...rows].map((row) => row.map(escape).join(",")).join("\n");
    const blob = new Blob([content], { type: "text/csv;charset=utf-8" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `${template.id}-${geoId.replaceAll(/[:|]/g, "-") || "place"}.csv`;
    link.click();
    URL.revokeObjectURL(link.href);
  }

  function handleSave() {
    saveChart({
      id: `profile:${template.id}:${geoId || "none"}`,
      version: 1,
      title: `${template.title} — ${placeName || geoId}`,
      chartType: "profile",
      template: template.id,
      geoId: geoId || null,
      metrics: availableMeasures.map((measure) => measure.metricCode),
      transformation: "none",
      savedAt: new Date().toISOString(),
    });
    setSaveStatus("Saved for Builder");
    window.setTimeout(() => setSaveStatus(""), 2400);
  }

  return (
    <main
      className="page-shell"
      data-testid="profile-product"
      data-template={template.id}
      data-geo-id={geoId}
      data-available-measures={coverage.available}
      data-unavailable-measures={coverage.unavailable}
    >
      <header className="page-heading">
        <div className="section-kicker">Product</div>
        <h1>{template.title}</h1>
        <p>{template.summary}</p>
        <p className="subtle" data-testid="template-limits">
          {template.limits}
        </p>
      </header>

      <section className="profile-controls">
        <label>
          Product
          <select
            value={templateId}
            onChange={(event) => setTemplateId(event.target.value)}
            data-testid="template-select"
          >
            {PRODUCT_TEMPLATES.map((item) => (
              <option value={item.id} key={item.id}>
                {item.title}
              </option>
            ))}
          </select>
        </label>
        <label>
          State
          <select
            value={stateFips}
            onChange={(event) => {
              setStateFips(event.target.value);
              setGeoId("");
            }}
            data-testid="profile-state"
          >
            <option value="">All states</option>
            {states.map((state) => (
              <option value={state.state_fips || ""} key={state.geo_id}>
                {state.state_name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Place
          <select
            value={geoId}
            onChange={(event) => setGeoId(event.target.value)}
            data-testid="profile-place"
          >
            <option value="">Select a place</option>
            {scopedCounties.map((county) => (
              <option value={county.geo_id} key={county.geo_id}>
                {county.county_name}
                {county.state_name ? `, ${county.state_name}` : ""}
              </option>
            ))}
          </select>
        </label>
        <button className="button secondary" type="button" onClick={exportCsv} data-testid="profile-export">
          <Download size={15} /> Export CSV
        </button>
        <button className="button primary" type="button" onClick={handleSave} data-testid="profile-save">
          <Save size={15} /> Save profile
        </button>
      </section>
      {saveStatus ? <div className="save-toast" role="status">{saveStatus}</div> : null}

      <section className="status-row">
        <StatusPill
          state={catalogStatus.state}
          label="Measures"
          message={catalogStatus.message}
          testId="profile-catalog-status"
        />
        <StatusPill
          state={observationStatus.state}
          label="Observations"
          message={observationStatus.message}
          testId="profile-observation-status"
        />
      </section>

      {coverage.unavailable > 0 ? (
        <p className="coverage-note partial" data-testid="profile-coverage-note">
          {coverage.unavailable} of {coverage.requested} measures in this product are not
          published by this warehouse. Their sections stay below with the gap stated: an
          absent measure is not the same as a place with nothing to report.
        </p>
      ) : null}

      {resolved.map((entry) => (
        <section className="analysis-panel" key={entry.section.id} data-testid={`section-${entry.section.id}`}>
          <div className="panel-heading">
            <div>
              <div className="section-kicker">{entry.section.title}</div>
              <p className="subtle">{entry.section.description}</p>
            </div>
          </div>
          <div className="profile-stats">
            {entry.measures.map((measure) => (
              <MeasureCard
                key={measure.slot.id}
                measure={measure}
                answer={answers[measure.slot.id]}
                geoId={geoId}
                placeName={placeName}
              />
            ))}
          </div>
        </section>
      ))}
    </main>
  );
}

function MeasureCard({
  measure,
  answer,
  geoId,
  placeName,
}: {
  measure: ResolvedMeasure;
  answer: MeasureAnswer | undefined;
  geoId: string;
  placeName: string;
}) {
  if (!measure.available) {
    return (
      <div data-testid={`measure-${measure.slot.id}`} data-available="false">
        <span>{measure.slot.label}</span>
        <strong>Not published</strong>
        <small data-testid={`measure-reason-${measure.slot.id}`}>{measure.reason}</small>
      </div>
    );
  }

  const metric = measure.metric;
  const row = answer?.row || null;
  const quality = metricQualityState(metric);

  return (
    <div data-testid={`measure-${measure.slot.id}`} data-available="true">
      <span>{measure.slot.label}</span>
      {/* A value the source did not publish is stated as such; the shared
          formatter never turns an absent value into a zero. */}
      <strong data-testid={`measure-value-${measure.slot.id}`}>
        {row && row.value !== null && row.value !== undefined
          ? `${formatObservationValue(row.value)} ${observationUnit(row)}`.trim()
          : "Not published for this place"}
      </strong>
      {row?.value_status ? (
        <small data-testid={`measure-status-${measure.slot.id}`}>
          Published status: {String(row.value_status)}
        </small>
      ) : null}
      <small>
        {/* The identity that answered, never only the slot's label. */}
        {metric ? displayMetricName(metric) : measure.metricCode} · {measure.metricCode}
      </small>
      <small>
        Source: {String(metric?.source_code ?? "not published")}
        {row ? ` · Period: ${observationPeriodLabel(row) || "not published"}` : ""}
      </small>
      {row ? <small>Margin of error: {marginOfErrorText(row)}</small> : null}
      <small>
        <StatusPill
          state={quality.state}
          label="Freshness"
          message={quality.label}
          testId={`measure-freshness-${measure.slot.id}`}
        />
      </small>
      {answer && !row ? (
        <small data-testid={`measure-answer-${measure.slot.id}`}>{answer.message}</small>
      ) : null}
      {measure.slot.note ? <small>{measure.slot.note}</small> : null}
      <small>
        <Link
          className="text-link"
          href={explorerHref({ metric: measure.metricCode, geoId: geoId || undefined })}
          data-testid={`measure-explore-${measure.slot.id}`}
        >
          Explore {placeName ? `for ${placeName}` : "this measure"} <ArrowRight size={13} />
        </Link>
      </small>
    </div>
  );
}
