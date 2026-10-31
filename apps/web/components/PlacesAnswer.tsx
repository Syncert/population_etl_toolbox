"use client";

// One measure in two places, as `/comparison/places` answers it
// (web-cross-level-compare, ADR-0010). The row asks the API, shows both
// values with their periods, says how the places relate, and when the API
// refuses a total across levels it says why in plain words and shows the
// share or per-resident view the API offers instead.

import { useEffect, useState } from "react";
import { apiErrorMessage } from "../lib/api/client";
import {
  containmentSentence,
  differenceText,
  getPlacesComparison,
  noteSentences,
  refusalSentence,
  sideBar,
  sideMarginText,
  sidePeriod,
  sideValue,
  sideValueText,
} from "../lib/crossLevelComparison";
import type { ComparedSide, ComparisonAlternative, PlacesComparison } from "../lib/crossLevelComparison";
import { DRAFT_VIEW_TEXT } from "../lib/derivedViews";
import type { DerivedRatioPairing } from "../lib/derivedViews";

interface Asked {
  /** A count's or rate's own code, or a pairing's ratio id. */
  metricCode?: string;
  ratioId?: string;
}

interface Answered {
  state: "loading" | "ok" | "error";
  answer?: PlacesComparison;
  /** The view the API offered for a refused total, and its answer. */
  instead?: { alternative: ComparisonAlternative; answer: PlacesComparison };
  error?: string;
}

export default function PlacesAnswer({
  measureId,
  label,
  metricCode,
  pairing,
  unit,
  a,
  b,
}: {
  measureId: string;
  label: string;
  metricCode: string;
  /** The per-capita or share pairing the chapter's view chose, if any. */
  pairing: DerivedRatioPairing | null;
  unit: string;
  a: ComparedSide;
  b: ComparedSide;
}) {
  const [answered, setAnswered] = useState<Answered>({ state: "loading" });
  const asked: Asked = pairing ? { ratioId: pairing.ratio_id } : { metricCode };

  useEffect(() => {
    const controller = new AbortController();
    const { signal } = controller;
    const ask = (question: Asked) =>
      getPlacesComparison(
        { ...(question.ratioId ? { ratio_id: question.ratioId } : { metric_code: question.metricCode }), geo_id_a: a.geoId, geo_id_b: b.geoId },
        { signal },
      );
    setAnswered({ state: "loading" });
    (async () => {
      try {
        const answer = await ask(asked);
        const alternative = answer.refusal?.reason === "counts_differ_in_size" ? answer.refusal.alternatives?.[0] : undefined;
        if (!alternative) {
          if (!signal.aborted) setAnswered({ state: "ok", answer });
          return;
        }
        const instead = await ask({ ratioId: alternative.ratio_id });
        if (!signal.aborted) setAnswered({ state: "ok", answer, instead: { alternative, answer: instead } });
      } catch (error) {
        if (!signal.aborted) setAnswered({ state: "error", error: apiErrorMessage(error) });
      }
    })();
    return () => controller.abort();
    // `asked` is derived from `pairing` and `metricCode`.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pairing?.ratio_id, metricCode, a.geoId, b.geoId]);

  const testId = `compare-row-${measureId}`;
  if (answered.state !== "ok" || !answered.answer) {
    return (
      <div className="paired-row" data-testid={testId} data-state={answered.state}>
        <h3>{pairing ? pairing.label : label}</h3>
        <p className="subtle">{answered.state === "loading" ? "Reading both places…" : `This comparison could not be read: ${answered.error}`}</p>
      </div>
    );
  }
  const { answer, instead } = answered;
  const shown = answer.comparable ? answer : instead?.answer.comparable ? instead.answer : null;
  // A side with no value is refused (`not_published`); the side that has
  // one is still shown, beside "Not published for this place".
  const unpublished = !shown ? [answer, instead?.answer].find((item) => item?.refusal?.reason === "not_published") ?? null : null;
  return (
    <div
      className="paired-row"
      data-testid={testId}
      data-state={shown ? "ok" : "refused"}
      data-measure-kind={answer.measure_kind}
      data-kind-status={answer.measure_kind_status}
      data-refusal={answer.refusal?.reason}
      data-ratio-id={shown?.ratio_id || undefined}
    >
      <h3>{pairing ? pairing.label : label}</h3>
      {!answer.comparable ? (
        <p data-testid={`${testId}-refusal`} title={answer.refusal?.detail}>
          {refusalSentence(answer, a, b)}
        </p>
      ) : null}
      {shown ? (
        <AnsweredValues
          answer={shown}
          heading={shown === answer ? null : instead!.alternative.label}
          unit={shown === answer ? (pairing ? pairing.unit : unit) : instead!.alternative.unit}
          draft={shown === answer ? pairing?.status : "draft"}
          a={a}
          b={b}
          testId={testId}
        />
      ) : null}
      {unpublished ? (
        <AnsweredValues
          answer={unpublished}
          heading={unpublished === answer ? null : instead!.alternative.label}
          unit={unpublished === answer ? (pairing ? pairing.unit : unit) : instead!.alternative.unit}
          draft={unpublished === answer ? pairing?.status : "draft"}
          a={a}
          b={b}
          testId={testId}
        />
      ) : null}
      {instead && !instead.answer.comparable ? (
        <p className="subtle" data-testid={`${testId}-instead-refused`}>{refusalSentence(instead.answer, a, b)}</p>
      ) : null}
    </div>
  );
}

function AnsweredValues({
  answer,
  heading,
  unit,
  draft,
  a,
  b,
  testId,
}: {
  answer: PlacesComparison;
  heading: string | null;
  unit: string;
  draft?: string;
  a: ComparedSide;
  b: ComparedSide;
  testId: string;
}) {
  const digits = answer.derived ? 2 : 1;
  const sides = [
    { place: a, side: answer.side_a },
    { place: b, side: answer.side_b },
  ];
  const periods = [...new Set(sides.map(({ side }) => sidePeriod(side)).filter(Boolean))];
  const containment = answer.comparable ? containmentSentence(answer, a, b) : "";
  const notes = answer.comparable ? noteSentences(answer, a, b) : [];
  const difference = answer.comparable ? differenceText(answer, a, b, unit, digits) : "";
  return (
    <div data-testid={`${testId}-values`} data-period-basis={answer.period_basis || undefined}>
      {heading ? (
        <p data-testid={`${testId}-instead`}>
          <strong>Shown instead: {heading}</strong>
          {draft && draft !== "approved" ? <em className="subtle"> ({DRAFT_VIEW_TEXT})</em> : null}
        </p>
      ) : null}
      <p className="subtle">
        {periods.length === 1 ? periods[0] : periods.join(" and ")}
        {unit ? ` · ${unit}` : ""}
        {!heading && draft && draft !== "approved" ? <em> ({DRAFT_VIEW_TEXT})</em> : null}
      </p>
      {sides.map(({ place, side }) => {
        const value = sideValue(side);
        const margin = sideMarginText(side, digits);
        return (
          <div key={place.geoId} className="paired-bar" data-geo-id={place.geoId} data-published={value === null ? "false" : "true"}>
            <span className="paired-name">{place.name}</span>
            <span className="paired-track" aria-hidden="true">
              {value !== null ? <span className="paired-fill" style={{ width: `${Math.max(2, sideBar(value, answer))}%` }} /> : null}
            </span>
            <span className="paired-value">
              {sideValueText(side, unit, digits)}
              {margin ? <span className="subtle paired-margin"> {margin}</span> : null}
              {periods.length > 1 && value !== null ? <span className="subtle"> · {sidePeriod(side)}</span> : null}
            </span>
          </div>
        );
      })}
      {containment ? <p className="subtle" data-testid={`${testId}-containment`}>{containment}</p> : null}
      {notes.map((note) => (
        <p key={note} className="subtle" data-testid={`${testId}-note`}>{note}</p>
      ))}
      {difference ? <p className="subtle" data-testid={`${testId}-difference`}>{difference}</p> : null}
    </div>
  );
}
