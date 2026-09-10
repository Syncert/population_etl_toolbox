// The bearer token's only home in the browser.
//
// The saved-analysis screen owned this privately, which was correct while it
// was the only screen that authenticated. Now that the explorer and the
// comparison workspace save to the account too, a second copy of the storage
// key would be a second place for the discipline below to drift out of.
//
// That discipline, from the saved-analysis contract (WEB-022):
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

/** The remembered token, or `""` when there is none or storage is unavailable. */
export function readStoredToken(): string {
  try {
    return window.sessionStorage.getItem(TOKEN_SESSION_KEY) || "";
  } catch {
    return "";
  }
}

/** Remember a token for this tab, or forget it when the value is empty. */
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
}

export function clearStoredToken(): void {
  storeToken("");
}

/**
 * The remembered token, read once after mount.
 *
 * Read after mount rather than during render because the server render has no
 * `sessionStorage`; reading during render would produce a hydration mismatch
 * on every authenticated screen. A screen that mounts already signed in
 * therefore renders its signed-out state for one frame, which is why callers
 * report "checking" rather than "not signed in" until this resolves.
 */
export function useStoredToken(): { token: string; resolved: boolean } {
  const [state, setState] = useState<{ token: string; resolved: boolean }>({
    token: "",
    resolved: false,
  });

  useEffect(() => {
    setState({ token: readStoredToken(), resolved: true });
  }, []);

  return state;
}
