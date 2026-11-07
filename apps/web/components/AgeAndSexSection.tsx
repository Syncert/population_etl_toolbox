"use client";

// "Age and sex" in a place page's People chapter (web-age-and-sex-structure).
//
// One `/age-structure` request on load answers the place and every level it
// sits in. The "Compare with" toggle (Nick, 2026-10-31) offers those levels
// in the API's order, next level up selected, plus None; a choice redraws
// the pyramid's outline, the cards' comparison values and the difference
// chart from that one answer, with no new request. `/history` is read once,
// only when the reader opens the change over time or the earlier outline;
// `/cohorts` once, only when the reader asks to follow the age groups.

import { useEffect, useMemo, useState } from "react";

import AgePyramid, { AgePyramidTable } from "./AgePyramid";
import AgeDifferenceBars from "./AgeDifferenceBars";
import Link from "next/link";
import AgeSummaryCards from "./AgeSummaryCards";
import { ageMapPathForPlace } from "../lib/ageMap";
import AgeStructureHistory from "./AgeStructureHistory";
import CohortChart from "./CohortChart";
import { ApiError, apiErrorMessage } from "../lib/api/client";
import {
  NO_REFERENCE,
  bandLines,
  cohortFromRelease,
  defaultReferenceKey,
  earlierReleases,
  getAgeCohorts,
  getAgeStructure,
  getAgeStructureHistory,
  isDrawable,
  periodText,
  placeOf,
  referenceChoices,
  referenceFor,
  unavailableText,
} from "../lib/ageStructure";
import type { AgeCohortsResponse, AgeHistoryResponse, AgeStructureResponse } from "../lib/ageStructure";

type Load<T> = { state: "idle" } | { state: "loading" } | { state: "error"; message: string } | { state: "ready"; value: T };

export default function AgeAndSexSection({ geoId, testId = "age-section" }: { geoId: string; testId?: string }) {
  const [answer, setAnswer] = useState<Load<AgeStructureResponse | null>>({ state: "loading" });
  const [referenceKey, setReferenceKey] = useState<string>(NO_REFERENCE);
  const [view, setView] = useState<"chart" | "table">("chart");
  const [history, setHistory] = useState<Load<AgeHistoryResponse>>({ state: "idle" });
  const [showHistory, setShowHistory] = useState(false);
  const [earlierRelease, setEarlierRelease] = useState<string>("");
  const [cohorts, setCohorts] = useState<Load<AgeCohortsResponse>>({ state: "idle" });

  useEffect(() => {
    const controller = new AbortController();
    setAnswer({ state: "loading" });
    setHistory({ state: "idle" });
    setCohorts({ state: "idle" });
    setShowHistory(false);
    setEarlierRelease("");
    getAgeStructure({ geo_id: geoId }, { signal: controller.signal })
      .then((payload) => {
        if (controller.signal.aborted) return;
        setAnswer({ state: "ready", value: payload });
        setReferenceKey(defaultReferenceKey(payload));
      })
      .catch((error) => {
        if (controller.signal.aborted) return;
        // An API without the route yet, or a place the survey does not
        // publish: the section is left out rather than shown broken.
        if (error instanceof ApiError && (error.status === 404 || error.status === 503)) {
          setAnswer({ state: "ready", value: null });
          return;
        }
        setAnswer({ state: "error", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, [geoId]);

  const payload = answer.state === "ready" ? answer.value : null;
  const place = payload ? placeOf(payload) : null;
  const choices = useMemo(() => (payload ? referenceChoices(payload) : []), [payload]);
  const reference = payload ? referenceFor(payload, referenceKey) : null;
  const drawnReference = isDrawable(reference) ? reference : null;
  const earlierChoices = history.state === "ready" && place ? earlierReleases(history.value, place.release) : [];
  const earlier = earlierChoices.find((item) => item.release === earlierRelease) || null;

  function loadHistory() {
    if (history.state !== "idle") return;
    setHistory({ state: "loading" });
    getAgeStructureHistory({ geo_id: geoId, include_bands: true })
      .then((value) => {
        setHistory({ state: "ready", value });
      })
      .catch((error) => setHistory({ state: "error", message: apiErrorMessage(error) }));
  }

  function loadCohorts() {
    if (cohorts.state !== "idle" || !place) return;
    const from = cohortFromRelease(place.release);
    if (!from || !place.release) return;
    setCohorts({ state: "loading" });
    getAgeCohorts({ geo_id: geoId, from_release: from, to_release: place.release })
      .then((value) => setCohorts({ state: "ready", value }))
      .catch((error) => setCohorts({ state: "error", message: apiErrorMessage(error) }));
  }

  if (answer.state === "loading") {
    return <p className="subtle" data-testid={`${testId}-loading`}>Reading who lives here by age and sex…</p>;
  }
  if (answer.state === "error") {
    return <p className="notice" data-testid={`${testId}-error`}>{`Age and sex could not be read: ${answer.message}`}</p>;
  }
  if (!payload || !place) return null;
  const refused: { geo_name: string; unavailable_reason: string | null } = place;
  if (!isDrawable(place)) {
    return (
      <section className="age-section" data-testid={testId} data-state="unavailable" aria-labelledby={`${testId}-heading`}>
        <h3 id={`${testId}-heading`}>Age and sex</h3>
        <p className="subtle">{`Age and sex for ${refused.geo_name}: ${unavailableText(refused.unavailable_reason)}.`}</p>
      </section>
    );
  }

  const period = periodText(place);
  const selected = choices.find((choice) => choice.key === referenceKey) || null;

  return (
    <section
      className="age-section"
      data-testid={testId}
      data-state="ready"
      data-reference={referenceKey}
      aria-labelledby={`${testId}-heading`}
    >
      <h3 id={`${testId}-heading`}>{`Age and sex, ${period}`}</h3>
      <p className="subtle">
        Each bar is that age group&apos;s share of everyone who lives in {place.geo_name}. Men are on the left, women on the
        right. The dark outline is the comparison drawn on the same scale.
      </p>

      <div className="command-row age-controls">
        {choices.length ? (
          <div className="age-control" data-testid={`${testId}-reference`}>
            <strong id={`${testId}-reference-label`}>Compare with</strong>
            <div className="segmented-control" role="group" aria-labelledby={`${testId}-reference-label`}>
              {choices.map((choice) => (
                <button
                  key={choice.key}
                  type="button"
                  className={choice.key === referenceKey ? "selected" : undefined}
                  aria-pressed={choice.key === referenceKey}
                  data-choice={choice.key}
                  data-level={choice.levelWord}
                  title={choice.overlapNote || undefined}
                  onClick={() => setReferenceKey(choice.key)}
                >
                  {choice.key === NO_REFERENCE ? "None" : `${choice.levelWord}: ${choice.name}`}
                </button>
              ))}
            </div>
          </div>
        ) : null}
        <div className="age-control">
          <strong id={`${testId}-view-label`}>Show as</strong>
          <div className="segmented-control" role="group" aria-labelledby={`${testId}-view-label`} data-testid={`${testId}-view`}>
            {(["chart", "table"] as const).map((choice) => (
              <button key={choice} type="button" className={choice === view ? "selected" : undefined} aria-pressed={choice === view} onClick={() => setView(choice)}>
                {choice === "chart" ? "Chart" : "Table"}
              </button>
            ))}
          </div>
        </div>
        <label className="age-control age-earlier" data-testid={`${testId}-earlier`}>
          <input
            type="checkbox"
            checked={Boolean(earlierRelease)}
            onChange={(event) => {
              if (!event.target.checked) {
                setEarlierRelease("");
                return;
              }
              loadHistory();
              setEarlierRelease("pending");
            }}
          />{" "}
          Outline an earlier survey
        </label>
        {earlierRelease && earlierChoices.length > 1 ? (
          <label className="age-control">
            <span className="subtle">Earlier survey</span>
            <select value={earlier?.release || ""} onChange={(event) => setEarlierRelease(event.target.value)} data-testid={`${testId}-earlier-release`}>
              {earlierChoices.map((item) => <option key={item.release || ""} value={item.release || ""}>{periodText(item)}</option>)}
            </select>
          </label>
        ) : null}
      </div>
      <EarlierPending
        wanted={Boolean(earlierRelease)}
        history={history}
        hasChoice={earlierChoices.length > 0}
        pick={() => setEarlierRelease(earlierChoices[earlierChoices.length - 1]?.release || "")}
        current={earlier?.release || ""}
        testId={`${testId}-earlier-state`}
      />

      {selected?.overlapNote ? (
        <p className="subtle" data-testid={`${testId}-overlap`}>{`${selected.overlapNote}, so the comparison covers some people outside it.`}</p>
      ) : null}
      {reference && !drawnReference ? (
        <p className="notice" data-testid={`${testId}-reference-unavailable`}>
          {`${reference.geo_name}: ${unavailableText(reference.unavailable_reason || "not_published")}, so nothing is outlined.`}
        </p>
      ) : null}

      <div className="age-main">
        <div className="age-figure">
          {view === "chart" ? (
            <AgePyramid place={place} reference={drawnReference} earlier={earlier} testId={`${testId}-pyramid`} />
          ) : (
            <AgePyramidTable place={place} reference={drawnReference} testId={`${testId}-table`} />
          )}
        </div>
        <AgeSummaryCards place={place} reference={drawnReference} scheme={payload.scheme} testId={`${testId}-cards`} />
      </div>
      {place.geo_level === "NATIONAL" || place.geo_level === "STATE" ? null : (
        <p className="subtle">
          <Link className="text-link" href={ageMapPathForPlace("median_age", place.geo_id, place.geo_level)} data-testid={`${testId}-map-link`}>
            See these figures on a map
          </Link>
        </p>
      )}

      {drawnReference ? (
        <div className="age-subsection" data-testid={`${testId}-differs`}>
          <h4>{`Where ${place.geo_name} differs from ${drawnReference.geo_name}`}</h4>
          <AgeDifferenceBars place={place} reference={drawnReference} testId={`${testId}-difference`} />
        </div>
      ) : null}

      <div className="age-subsection">
        {showHistory ? (
          <>
            <h4>Change over time</h4>
            {history.state === "ready" ? <AgeStructureHistory history={history.value} testId={`${testId}-history`} /> : null}
            {history.state === "loading" ? <p className="subtle">Reading earlier surveys…</p> : null}
            {history.state === "error" ? <p className="notice">{`Earlier surveys could not be read: ${history.message}`}</p> : null}
          </>
        ) : (
          <button type="button" className="button secondary" data-testid={`${testId}-history-open`} onClick={() => { setShowHistory(true); loadHistory(); }}>
            Show change in the broad age groups over time
          </button>
        )}
      </div>

      <div className="age-subsection">
        {cohorts.state === "idle" ? (
          cohortFromRelease(place.release) ? (
            <button type="button" className="button secondary" data-testid={`${testId}-cohorts-open`} onClick={loadCohorts}>
              Follow each age group forward five years
            </button>
          ) : null
        ) : (
          <>
            <h4>Each age group five years on</h4>
            {cohorts.state === "ready" ? <CohortChart cohorts={cohorts.value} testId={`${testId}-cohorts`} /> : null}
            {cohorts.state === "loading" ? <p className="subtle">Reading the earlier survey…</p> : null}
            {cohorts.state === "error" ? <p className="notice">{`The earlier survey could not be read: ${cohorts.message}`}</p> : null}
          </>
        )}
      </div>

      <p className="subtle age-provenance">
        {`American Community Survey ${payload.dataset === "acs1" ? "1-year" : "5-year"}, ${period}. `}
        The Census Bureau publishes 23 age groups of unequal width; they are added into 18 five-year groups, and the margin of
        each sum is the square root of the sum of squared margins, as the Bureau instructs. Whiskers are 90 % margins of
        error. Median age is the Bureau&apos;s own figure. Dependency ratios are people under 18, or 65 and over, per 100
        people aged 18 to 64.
      </p>
      <details className="age-provenance-lines">
        <summary>The published lines behind each age group</summary>
        <p className="subtle" data-provenance="age-bands">{place.bands.map((band) => `${band.label}: ${bandLines(band)}`).join("; ")}</p>
      </details>
    </section>
  );
}

/** Settles the "Outline an earlier survey" box once the history has arrived. */
function EarlierPending({
  wanted,
  history,
  hasChoice,
  pick,
  current,
  testId,
}: {
  wanted: boolean;
  history: Load<AgeHistoryResponse>;
  hasChoice: boolean;
  pick: () => void;
  current: string;
  testId: string;
}) {
  useEffect(() => {
    if (wanted && !current && history.state === "ready" && hasChoice) pick();
  }, [wanted, current, history.state, hasChoice, pick]);
  if (!wanted) return null;
  if (history.state === "loading") return <p className="subtle" data-testid={testId}>Reading earlier surveys…</p>;
  if (history.state === "error") return <p className="notice" data-testid={testId}>{`Earlier surveys could not be read: ${history.message}`}</p>;
  if (history.state === "ready" && !hasChoice) {
    return <p className="subtle" data-testid={testId}>No earlier survey period that does not overlap this one is published for this place.</p>;
  }
  return null;
}
