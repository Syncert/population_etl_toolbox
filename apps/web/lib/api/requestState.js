// Shared request lifecycle vocabulary and stale-response protection.
//
// The five states below are the ones the current UI renders; the reserved
// list names the additional distinct states the first-wave plan requires so
// new surfaces adopt one vocabulary instead of inventing strings.

export const REQUEST_STATES = Object.freeze({
  idle: "idle",
  loading: "loading",
  ok: "ok",
  warn: "warn",
  bad: "bad",
});

export const RESERVED_REQUEST_STATES = Object.freeze([
  "empty",
  "partial",
  "stale",
  "suppressed",
  "incompatible",
  "unauthorized",
  "forbidden",
  "rate-limited",
  "unavailable",
]);

// Guards effects against out-of-order async completions. Each begin()
// invalidates every earlier request; only the newest request's results may
// be committed to state.
export function createRequestTracker() {
  let current = 0;

  return {
    begin() {
      current += 1;
      const token = current;
      return {
        token,
        isCurrent: () => token === current,
      };
    },
    invalidate() {
      current += 1;
    },
  };
}
