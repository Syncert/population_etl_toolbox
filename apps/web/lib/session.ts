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
  return apiFetch<AccountResponse>("/account", {
    ...options,
    token: readSessionCredential(),
  });
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
