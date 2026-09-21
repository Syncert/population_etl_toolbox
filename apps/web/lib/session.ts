// The sign-in flow, from the browser's side (ADR-0005 §1-§2).
//
// Four things happen here and nowhere else: starting a sign-in, completing one,
// rotating a session, and ending one. Everything else in the application asks
// `apiToken` for a credential and does not know how it got there.
//
// **The authorization code never enters a URL this application controls.** The
// provider returns the browser to `/auth/callback` with `code` and `state` in
// the query — that part is the protocol and cannot be avoided — and the first
// thing the callback route does is hand them to the API in a request body and
// replace its own URL. A code left in the address bar travels into history,
// into a `Referer`, and into whatever a reader pastes when they ask for help.
//
// **The refresh token is never touched here.** It is an `HttpOnly` cookie
// scoped to `/api/v1/auth/refresh`; `fetch` attaches it to that one path
// because the request is same-origin, and script cannot read it. Code that
// tried to would not compile against anything, which is the point.

import { ApiError, apiFetch, type RequestOptions } from "./api/client";
import {
  clearSessionCredential,
  readSessionCredential,
  sessionExpiresAt,
  setSessionCredential,
} from "./apiToken";

/** What the API answers for a completed sign-in or a rotation. */
export interface SessionResponse {
  access_token: string;
  token_type: string;
  expires_at: string;
  expires_in: number;
}

export interface AccountResponse {
  public_display_name: string | null;
  email: string | null;
  created_at: string;
}

type Transport = Pick<RequestOptions, "fetchImpl" | "signal">;

/**
 * The URL the provider returns the browser to.
 *
 * Built from the live origin rather than configured, so a deployment cannot
 * hold a value that disagrees with where it is actually served. The API checks
 * it against an exact-match allowlist regardless, and refuses anything not on
 * it — so a wrong value here is a refused sign-in rather than a redirect
 * somewhere unintended.
 */
export function callbackUrl(origin: string = window.location.origin): string {
  return `${origin}/auth/callback`;
}

function hold(session: SessionResponse): SessionResponse {
  // `expires_at` is the API's own answer for when this token stops working.
  // Preferred over `Date.now() + expires_in * 1000`, which adds the round trip
  // to the lifetime and leaves the browser believing in a token the API has
  // already stopped accepting.
  const expiresAt = Date.parse(session.expires_at);
  setSessionCredential(
    session.access_token,
    Number.isFinite(expiresAt) ? expiresAt : Date.now() + session.expires_in * 1000,
  );
  return session;
}

/** Start a sign-in; returns where to send the browser. */
export async function startSignIn(
  redirectUri: string = callbackUrl(),
  options: Transport = {},
): Promise<string> {
  const started = await apiFetch<{ authorization_url: string }>("/auth/sign-in", {
    ...options,
    method: "POST",
    body: { redirect_uri: redirectUri },
  });
  return started.authorization_url;
}

/** Finish a sign-in. The credential is held on success and nowhere written. */
export async function completeSignIn(
  code: string,
  state: string,
  options: Transport = {},
): Promise<SessionResponse> {
  const session = await apiFetch<SessionResponse>("/auth/callback", {
    ...options,
    method: "POST",
    body: { code, state },
  });
  return hold(session);
}

/**
 * Rotate the session using the refresh cookie.
 *
 * Answers `false` rather than throwing when there is no live session to
 * rotate: "not signed in" is the ordinary state of most visitors, and a page
 * that calls this on mount should not have to catch an exception to learn it.
 * A failure for any other reason is still a failure.
 */
export async function refreshSession(options: Transport = {}): Promise<boolean> {
  try {
    const session = await apiFetch<SessionResponse>("/auth/refresh", {
      ...options,
      method: "POST",
    });
    hold(session);
    return true;
  } catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      clearSessionCredential();
      return false;
    }
    throw error;
  }
}

/** End this session. The credential is dropped even if the call fails. */
export async function signOut(options: Transport = {}): Promise<void> {
  const token = readSessionCredential();
  try {
    if (token) {
      await apiFetch<void>("/auth/sign-out", {
        ...options,
        method: "POST",
        token,
      });
    }
  } finally {
    // Dropped whatever happened. A sign-out that left the credential held
    // because the network was down would show a reader as signed in when they
    // have asked not to be, which is the one outcome worth ruling out.
    clearSessionCredential();
  }
}

/** End every session this account holds, on every device. */
export async function signOutEverywhere(options: Transport = {}): Promise<void> {
  const token = readSessionCredential();
  try {
    if (token) {
      await apiFetch<void>("/auth/sign-out-everywhere", {
        ...options,
        method: "POST",
        token,
      });
    }
  } finally {
    clearSessionCredential();
  }
}

export function getAccount(options: Transport = {}): Promise<AccountResponse> {
  return withFreshSession(
    (token) => apiFetch<AccountResponse>("/account", { ...options, token }),
    options,
  );
}

/**
 * When to rotate a session, as milliseconds before the access token expires.
 *
 * Rotating *before* it expires is what keeps a reader's next action from
 * failing. Waiting for the 401 works too and is the fallback below, but it
 * spends the reader's click: they press save, it fails, and whether they get
 * their work back depends on the screen.
 *
 * A minute is enough for a rotation to complete on a slow connection and
 * short enough that a session is not being refreshed constantly.
 */
export const ROTATE_BEFORE_EXPIRY_MS = 60_000;

/**
 * Keep the held session live, rotating shortly before it expires.
 *
 * Returns a cancel function. Scheduled rather than polled: there is exactly
 * one moment worth waking up for and it is known in advance.
 *
 * A background tab defeats this on its own, because browsers throttle timers
 * there — which is why it is a complement to `withFreshSession` rather than a
 * replacement for it. Between them: the common case costs the reader nothing,
 * and the throttled case costs one extra round trip.
 */
export function maintainSession(
  onChange: () => void = () => {},
  options: Transport = {},
): () => void {
  let timer: ReturnType<typeof setTimeout> | null = null;
  let cancelled = false;
  let previousExpiry = 0;

  const schedule = () => {
    if (cancelled) return;
    const expiresAt = sessionExpiresAt();
    if (expiresAt === null) return;
    // Stop unless the session actually moved forward. A deployment configured
    // with an access-token lifetime shorter than the window above would
    // otherwise compute a zero delay every time and rotate in a tight loop —
    // against the endpoint that is deliberately the most rate-limited on the
    // API. One rotation that does not extend anything is enough to know that
    // rotating again will not either.
    if (expiresAt <= previousExpiry) return;
    previousExpiry = expiresAt;
    const delay = Math.max(0, expiresAt - Date.now() - ROTATE_BEFORE_EXPIRY_MS);
    timer = setTimeout(async () => {
      if (cancelled) return;
      try {
        await refreshSession(options);
      } catch {
        // A failed rotation is not this function's to report: the next
        // authenticated call will find out, and `withFreshSession` will try
        // once more. Throwing from a timer would be an unhandled rejection.
      }
      onChange();
      schedule();
    }, delay);
  };

  schedule();
  return () => {
    cancelled = true;
    if (timer !== null) clearTimeout(timer);
  };
}

/**
 * Run an authenticated call, rotating once if the credential has expired.
 *
 * A fifteen-minute access token means a reader who leaves a tab open over
 * lunch comes back to a `401` on their next save. Rotating and retrying once
 * is what makes that invisible; rotating and retrying *more* than once would
 * turn a genuinely revoked session into a loop against an endpoint that is
 * deliberately rate-limited.
 *
 * Only a session is retried. An operator token does not rotate — there is
 * nothing to refresh it with — so a `401` on one is final and is reported as
 * it always was.
 */
export async function withFreshSession<T>(
  run: (token: string) => Promise<T>,
  options: Transport = {},
): Promise<T> {
  const held = readSessionCredential();
  if (!held) {
    // No live session: either it expired while the tab was idle, or there
    // never was one. A rotation distinguishes them, and costs one request.
    const rotated = await refreshSession(options);
    if (!rotated) {
      throw new ApiError({
        status: 401,
        detail: "not signed in",
        path: "/api/v1/auth/refresh",
      });
    }
    return run(readSessionCredential());
  }

  try {
    return await run(held);
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 401) {
      throw error;
    }
    const rotated = await refreshSession(options);
    if (!rotated) {
      throw error;
    }
    return run(readSessionCredential());
  }
}
