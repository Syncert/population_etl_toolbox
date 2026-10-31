// The count / per-capita / share toggle and the derived value it shows
// (web-per-capita-toggle, ADR-0009). Every number here is the API's; the
// denominator is named beside each one, and a pairing nobody has reviewed
// yet says so, the way a draft plain label does (D-2).

import {
  DRAFT_VIEW_TEXT,
  agenciesText,
  choiceLabel,
  coverageText,
  denominatorText,
  derivedMarginText,
  derivedPeriodText,
  derivedValueText,
  draftViewNote,
  isDraft,
} from "../lib/derivedViews";
import type { DerivedRatioObservation, DerivedRatioPairing, ViewChoice } from "../lib/derivedViews";

export function ViewToggle({
  choices,
  value,
  onChange,
  pairings,
  label,
  testId,
}: {
  choices: readonly ViewChoice[];
  value: ViewChoice;
  onChange: (choice: ViewChoice) => void;
  pairings: readonly DerivedRatioPairing[];
  label: string;
  testId: string;
}) {
  if (choices.length < 2) return null;
  return (
    <div className="command-row derived-toggle" data-testid={testId}>
      <strong id={`${testId}-label`}>{label}</strong>
      <div className="segmented-control" role="group" aria-labelledby={`${testId}-label`}>
        {choices.map((choice) => (
          <button
            key={choice}
            type="button"
            className={choice === value ? "selected" : undefined}
            aria-pressed={choice === value}
            data-choice={choice}
            onClick={() => onChange(choice)}
          >
            {choiceLabel(choice, pairings)}
          </button>
        ))}
      </div>
    </div>
  );
}

export function DraftViewMark({ pairing, testId }: { pairing: Pick<DerivedRatioPairing, "status" | "version">; testId?: string }) {
  if (!isDraft(pairing.status)) return null;
  return (
    <em className="subtle" title={draftViewNote(pairing)} data-pairing-status={pairing.status} data-testid={testId}>
      ({DRAFT_VIEW_TEXT})
    </em>
  );
}

/** The denominator a provider's own rate was computed on, named under its card. */
export function ProviderRateNote({ pairing, testId }: { pairing: DerivedRatioPairing; testId: string }) {
  return (
    <p className="subtle" data-testid={testId} data-ratio-id={pairing.ratio_id}>
      {sentence(pairing.unit)}, {denominatorText(pairing)}.{" "}
      <DraftViewMark pairing={pairing} />
    </p>
  );
}

function sentence(text: string): string {
  return text ? text[0]!.toUpperCase() + text.slice(1) : text;
}

/** One derived value with its denominator, period, margin and review state. */
export function DerivedValue({
  pairing,
  row,
  pending,
  error,
  testId,
}: {
  pairing: DerivedRatioPairing;
  row: DerivedRatioObservation | null;
  pending?: boolean;
  error?: string;
  testId?: string;
}) {
  if (pending) return <span data-testid={testId}>Reading…</span>;
  if (error) return <span data-testid={testId}>{error}</span>;
  if (!row) return <span data-testid={testId} data-available="false">Not published for this place</span>;
  const margin = derivedMarginText(row);
  const period = derivedPeriodText(row);
  const available = row.value !== null && row.value !== undefined && !row.unavailable_reason;
  return (
    <span
      data-testid={testId}
      data-ratio-id={pairing.ratio_id}
      data-available={available ? "true" : "false"}
      data-unavailable-reason={row.unavailable_reason || undefined}
      data-derived={row.derived ? "true" : "false"}
    >
      <strong>{derivedValueText(row)}</strong>{" "}
      <span className="subtle">{denominatorText(pairing)}</span>
      {period ? <span className="subtle"> · {period}</span> : null}
      {margin ? <span className="subtle"> · {margin}</span> : null}{" "}
      <DraftViewMark pairing={{ status: row.pairing_status, version: row.pairing_version }} />
    </span>
  );
}

/**
 * A county crime rate computed from agency reports (ADR-0012), with whom it
 * covers in plain words beside the figure, the agencies behind it on hover,
 * and a refused month's reason with the same coverage, never a zero.
 */
export function AgencyRateValue({
  pairing,
  row,
  pending,
  error,
  testId,
}: {
  pairing: DerivedRatioPairing;
  row: DerivedRatioObservation | null;
  pending?: boolean;
  error?: string;
  testId: string;
}) {
  if (pending) return <span data-testid={testId}>Reading…</span>;
  if (error) return <span data-testid={testId}>{error}</span>;
  if (!row) return <span data-testid={testId} data-available="false">No county rate from agency reports</span>;
  const period = derivedPeriodText(row);
  const available = row.value !== null && row.value !== undefined && !row.unavailable_reason;
  const agencies = agenciesText(row.coverage);
  return (
    <span
      data-testid={testId}
      data-ratio-id={pairing.ratio_id}
      data-available={available ? "true" : "false"}
      data-unavailable-reason={row.unavailable_reason || undefined}
      data-derived="true"
      title={agencies || undefined}
    >
      <strong>{derivedValueText(row)}</strong>{" "}
      <span className="subtle">computed from agency reports</span>
      {period ? <span className="subtle"> · {period}</span> : null}
      <span className="subtle" data-testid={`${testId}-coverage`}> · {coverageText(row.coverage)}</span>
      {agencies ? <span className="sr-only"> ({agencies})</span> : null}{" "}
      <DraftViewMark pairing={{ status: row.pairing_status, version: row.pairing_version }} />
    </span>
  );
}
