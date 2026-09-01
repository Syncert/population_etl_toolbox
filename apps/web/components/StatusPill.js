import { REQUEST_STATES } from "../lib/api/requestState";

// Failure-shaped states render as errors; everything not yet proven good
// renders as a caution so a stale or partial value can never present as
// current. Only a completed, healthy request earns the ok treatment.
const ERROR_STATES = new Set([
  "bad",
  "unauthorized",
  "forbidden",
  "rate-limited",
  "unavailable",
  "incompatible",
]);

export function pillClass(state) {
  if (state === REQUEST_STATES.ok) {
    return "pill ok";
  }
  if (ERROR_STATES.has(state)) {
    return "pill bad";
  }
  return "pill warn";
}

export default function StatusPill({ state, label, message, testId }) {
  return (
    <span className={pillClass(state)} data-testid={testId}>
      {label}: <strong>{message}</strong>
    </span>
  );
}
