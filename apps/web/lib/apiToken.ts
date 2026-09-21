// The bearer credential's only home in the browser.
//
// The saved-analysis screen owned this privately, which was correct while it
// was the only screen that authenticated. Now that the explorer and the
// comparison workspace save to the account too, a second copy of the storage
// key would be a second place for the discipline below to drift out of.
//
// ADR-0005 added a second *kind* of credential and, with it, the question this
// module now answers. There are two, they cannot share a storage mechanism,
// and the plan forbids them having two homes:
//
// - A **session access token**, from a self-service sign-in. ADR-0005 §2 says
//   it is held "only in JavaScript memory. It is never written to
//   `sessionStorage`, `localStorage`, or anywhere else that survives the
//   page." It dies with the tab and expires in fifteen minutes regardless;
//   what survives a reload is the `HttpOnly` refresh cookie, which this module
//   cannot see and must not try to.
// - An **operator token**, pasted by a reader who was given one. WEB-022 keeps
//   it in `sessionStorage`, and ADR-0005 §6 keeps it working unchanged.
//
// So the reconciliation is: one module, one accessor, two backings. Every
// screen calls `useStoredToken()` and gets whichever credential is held,
// without knowing or caring which — because what a screen does with it is
// identical either way. There is still exactly one place where a browser-held
// credential lives, which is the property scope item 4 asked for; what there
// is not is one *mechanism*, which ADR-0005 §2 forbids.
//
// The local store's role is unchanged and deliberately not retired:
// `lib/savedAnalysis.saveDestination` still picks the browser when no
// credential is held, and a signed-out reader's work still has somewhere to
// go. Signing in is now a way out of that, rather than the only way being to
// ask an operator for a token.
//
// The discipline, from the saved-analysis contract (WEB-022) and unchanged:
//
// - `sessionStorage` only, and only when the user asked this browser to
//   remember it. It is never written to `localStorage`, where it would sit
//   beside the public saved-chart store and outlive the tab.
// - Never into a URL, a link, a referrer, or history. A token in a query
//   string travels into server logs and shared links; it reaches the API only
//   as an `Authorization` header.
// - Never logged, and never included in an error message.
//
// Every access is guarded. Storage is not merely empty in a private window or
// with site data blocked — the accessor itself throws, and an unguarded read
// would take down the screen that called it.

import { useEffect, useState } from "react";

export const TOKEN_SESSION_KEY = "economic-data-studio:api-token";

/** Which kind of credential a screen is holding, for what it tells a reader. */
export type CredentialKind = "session" | "operator" | "none";

interface SessionCredential {
  token: string;
  /** When the access token stops working, as epoch milliseconds. */
  expiresAt: number;
}

// Module scope, deliberately. This is what "only in JavaScript memory" means:
// a closing tab takes it, a reload takes it, and nothing that reads storage
// can find it. The refresh cookie is what makes a reload recoverable, and it
// is `HttpOnly`, so it is not readable here by construction.
let sessionCredential: SessionCredential | null = null;

// Held credentials change outside React's knowledge — a sign-in completes on
// another route, a refresh rotates mid-request, a 401 clears one. Without a
// notification the five screens that authenticate would each keep rendering
// whatever they read on mount, so a reader would sign in and watch the page
// go on saying they had not.
const subscribers = new Set<() => void>();

function notify(): void {
  for (const subscriber of subscribers) {
    subscriber();
  }
}

/** Subscribe to credential changes; returns the unsubscribe. */
export function subscribeToCredential(listener: () => void): () => void {
  subscribers.add(listener);
  return () => {
    subscribers.delete(listener);
  };
}

/**
 * Hold a session access token in memory.
 *
 * `expiresAt` is the moment the API said it stops working, not a duration
 * computed here: the two differ by however long the response took, and the
 * side that knows is the one that minted it.
 */
export function setSessionCredential(token: string, expiresAt: number): void {
  sessionCredential = token ? { token, expiresAt } : null;
  notify();
}

export function clearSessionCredential(): void {
  sessionCredential = null;
  notify();
}

/**
 * The held session token, or `""`.
 *
 * An expired one is not returned. Presenting it would spend a request to learn
 * what its own `expires_at` already said, and the caller's next move — refresh
 * — is the same either way.
 */
export function readSessionCredential(now: number = Date.now()): string {
  if (!sessionCredential) {
    return "";
  }
  if (sessionCredential.expiresAt <= now) {
    return "";
  }
  return sessionCredential.token;
}

/** Whether a session token is held at all, expired or not. */
export function hasSessionCredential(): boolean {
  return sessionCredential !== null;
}

/** The remembered operator token, or `""` when there is none or storage is unavailable. */
export function readOperatorToken(): string {
  try {
    return window.sessionStorage.getItem(TOKEN_SESSION_KEY) || "";
  } catch {
    return "";
  }
}

/**
 * Whichever credential this browser holds.
 *
 * A session wins over a pasted operator token when both are present. An
 * operator who signs in has said which identity they mean by doing so, and the
 * alternative — preferring the older, longer-lived credential — would make
 * signing in appear to do nothing.
 */
export function readStoredToken(): string {
  return readSessionCredential() || readOperatorToken();
}

/** Which kind is held, for a screen that tells the reader where their work goes. */
export function heldCredentialKind(): CredentialKind {
  if (readSessionCredential()) {
    return "session";
  }
  return readOperatorToken() ? "operator" : "none";
}

/** Remember an operator token for this tab, or forget it when the value is empty. */
export function storeToken(token: string): void {
  try {
    if (token) {
      window.sessionStorage.setItem(TOKEN_SESSION_KEY, token);
    } else {
      window.sessionStorage.removeItem(TOKEN_SESSION_KEY);
    }
  } catch {
    // Storage unavailable: the token stays in memory for this page view only,
    // which is a weaker convenience but not a weaker boundary.
  }
  notify();
}

export function clearStoredToken(): void {
  storeToken("");
}

/**
 * The held credential, read after mount and kept current.
 *
 * Read after mount rather than during render because the server render has no
 * `sessionStorage`; reading during render would produce a hydration mismatch
 * on every authenticated screen. A screen that mounts already signed in
 * therefore renders its signed-out state for one frame, which is why callers
 * report "checking" rather than "not signed in" until this resolves.
 *
 * It then stays subscribed, so a sign-in completed on another route reaches
 * every screen that authenticates without any of them polling.
 */
export function useStoredToken(): {
  token: string;
  resolved: boolean;
  kind: CredentialKind;
} {
  const [state, setState] = useState<{
    token: string;
    resolved: boolean;
    kind: CredentialKind;
  }>({
    token: "",
    resolved: false,
    kind: "none",
  });

  useEffect(() => {
    const read = () =>
      setState({
        token: readStoredToken(),
        resolved: true,
        kind: heldCredentialKind(),
      });
    read();
    return subscribeToCredential(read);
  }, []);

  return state;
}
