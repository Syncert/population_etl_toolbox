// Shared request lifecycle vocabulary and stale-response protection.
//
// The five states below are the ones the current UI renders; the reserved
// list names the additional distinct states the first-wave plan requires so
// new surfaces adopt one vocabulary instead of inventing strings.

// Frozen at runtime as well as typed: a vocabulary any module could
// mutate is not a shared contract.
export const REQUEST_STATES = Object.freeze({
  idle: "idle",
  loading: "loading",
  ok: "ok",
  warn: "warn",
  bad: "bad",
} as const);

export type RequestState = (typeof REQUEST_STATES)[keyof typeof REQUEST_STATES];

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
] as const);

export type ReservedRequestState = (typeof RESERVED_REQUEST_STATES)[number];

/** Every state a status surface may be asked to render. */
export type AnyRequestState = RequestState | ReservedRequestState;

export interface TrackedRequest {
  token: number;
  /** False once a newer request has begun; results must then be discarded. */
  isCurrent: () => boolean;
}

export interface RequestTracker {
  begin: () => TrackedRequest;
  invalidate: () => void;
}

// Guards effects against out-of-order async completions. Each begin()
// invalidates every earlier request; only the newest request's results may
// be committed to state.
export function createRequestTracker(): RequestTracker {
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
