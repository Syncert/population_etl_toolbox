"use client";

// The compare page's age pyramids (web-age-and-sex-structure): both places
// from one `/age-structure` request (`compare_geo_ids`), drawn on one shared
// axis so the same bar length means the same share in each, each with the
// other outlined. Places in different survey periods are not set together.

import { useEffect, useState } from "react";

import AgePyramid from "./AgePyramid";
import { ApiError, apiErrorMessage } from "../lib/api/client";
import { comparedPlaces, getAgeStructure, isDrawable, periodText, placeOf, pyramidExtent } from "../lib/ageStructure";
import type { AgePlace, AgeStructureResponse } from "../lib/ageStructure";

type Load = { state: "loading" } | { state: "error"; message: string } | { state: "ready"; value: AgeStructureResponse | null };

export default function ComparePyramids({ a, b }: { a: string; b: string }) {
  const [answer, setAnswer] = useState<Load>({ state: "loading" });
  useEffect(() => {
    const controller = new AbortController();
    setAnswer({ state: "loading" });
    getAgeStructure({ geo_id: a, compare_geo_ids: [b] }, { signal: controller.signal })
      .then((value) => { if (!controller.signal.aborted) setAnswer({ state: "ready", value }); })
      .catch((error) => {
        if (controller.signal.aborted) return;
        if (error instanceof ApiError && (error.status === 404 || error.status === 503)) {
          setAnswer({ state: "ready", value: null });
          return;
        }
        setAnswer({ state: "error", message: apiErrorMessage(error) });
      });
    return () => controller.abort();
  }, [a, b]);

  if (answer.state === "loading") return null;
  if (answer.state === "error") {
    return <p className="notice" data-testid="compare-age-error">{`Age and sex could not be read: ${answer.message}`}</p>;
  }
  const payload = answer.value;
  const first = payload ? placeOf(payload) : null;
  const second: AgePlace | null = payload ? comparedPlaces(payload).find((item) => item.geo_id === b) || null : null;
  if (!payload || !isDrawable(first)) return null;
  const pair = isDrawable(second) ? second : null;
  const extent = pyramidExtent([first, pair]);
  return (
    <section className="analysis-panel age-section" aria-labelledby="compare-age-heading" data-testid="compare-age">
      <h2 id="compare-age-heading">Age and sex</h2>
      <p className="subtle">
        Each bar is that age group&apos;s share of everyone in the place, men on the left and women on the right. Both pyramids
        share one scale, and each outlines the other.
      </p>
      {second && !pair ? (
        <p className="notice" data-testid="compare-age-unavailable">{`${second.geo_name} is not published for ${periodText(first)}, so only ${first.geo_name} is drawn.`}</p>
      ) : null}
      <div className="age-compare-grid">
        <div>
          <h3>{`${first.geo_name}, ${periodText(first)}`}</h3>
          <AgePyramid place={first} reference={pair} extent={extent} testId="compare-age-a" />
        </div>
        {pair ? (
          <div>
            <h3>{`${pair.geo_name}, ${periodText(pair)}`}</h3>
            <AgePyramid place={pair} reference={first} extent={extent} testId="compare-age-b" />
          </div>
        ) : null}
      </div>
    </section>
  );
}
