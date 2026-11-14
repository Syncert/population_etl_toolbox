"use client";

// At a glance: median age (the Census Bureau's own figure), the shares under
// 18, 65 and over and 85 and over, the three dependency ratios and the sex
// ratio, each with its margin and the selected comparison's value. The
// figures computed here from the published age lines carry the draft mark
// while the grouping scheme is unreviewed; median age is published, so it
// never does.

import { AGE_CARDS, DRAFT_SCHEME_TEXT, draftSchemeNote, isDraftScheme } from "../lib/ageStructure";
import type { AgePlace, AgeScheme } from "../lib/ageStructure";

export default function AgeSummaryCards({
  place,
  reference,
  scheme,
  testId = "age-cards",
}: {
  place: AgePlace;
  reference: AgePlace | null;
  scheme: AgeScheme;
  testId?: string;
}) {
  const draft = isDraftScheme(scheme);
  return (
    <dl className="age-cards" data-testid={testId}>
      {AGE_CARDS.map((card) => {
        const own = card.read(place);
        const other = reference ? card.read(reference) : null;
        const extra = card.extra?.(place) || null;
        return (
          <div className="age-card" key={card.id} data-testid={`${testId}-${card.id}`}>
            <dt>
              {card.label}
              {card.derived && draft ? (
                <em className="draft-mark" title={draftSchemeNote(scheme)} data-scheme-status={scheme.status} data-testid={`${testId}-${card.id}-draft`}>
                  {DRAFT_SCHEME_TEXT}
                </em>
              ) : null}
            </dt>
            <dd>
              <span className="age-card-value" data-testid={`${testId}-${card.id}-value`}>{own.value}</span>{" "}
              <span className="subtle">{card.unit}</span>
              {own.moe ? <span className="subtle age-card-moe">{` ${own.moe}`}</span> : null}
            </dd>
            {extra ? <dd className="subtle">{extra}</dd> : null}
            {reference && other ? (
              <dd className="subtle" data-testid={`${testId}-${card.id}-reference`}>{`${reference.geo_name}: ${other.value}`}</dd>
            ) : null}
          </div>
        );
      })}
    </dl>
  );
}
