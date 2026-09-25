"use client";

// The sign-in control (ADR-0005 §1-§3).
//
// It renders in three states and says something true in each. The one worth
// naming is the middle one: on first paint nothing is known, because the
// credential lives in memory and a reload has only the `HttpOnly` refresh
// cookie to recover from — which means asking the API. Until that answer
// arrives the control says "checking", not "sign in", because telling a
// signed-in reader they are signed out is the kind of small lie that makes
// people re-enter credentials they did not need to.
//
// What it never renders: an account identifier, an issuer, a subject, or an
// operator label. A signed-in reader sees their own email or their own chosen
// public name, and a reader who has chosen neither sees the word "Account".

import { useCallback, useEffect, useState } from "react";

import { heldCredentialKind, useStoredToken } from "../lib/apiToken";
import {
  getAccount,
  maintainSession,
  refreshSession,
  signOut,
  startSignIn,
  type AccountResponse,
} from "../lib/session";
import { apiErrorMessage } from "../lib/api/client";

type Phase = "checking" | "signed-out" | "signed-in" | "working";

export default function SignInControl(): JSX.Element {
  const { resolved } = useStoredToken();
  const [phase, setPhase] = useState<Phase>("checking");
  const [account, setAccount] = useState<AccountResponse | null>(null);
  const [failure, setFailure] = useState<string>("");

  // One rotation on mount. This is the whole of "stay signed in across a
  // reload": the access token died with the previous page and the refresh
  // cookie is the only thing that survived it.
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const rotated = await refreshSession();
        if (cancelled) return;
        if (!rotated) {
          setPhase("signed-out");
          return;
        }
        const record = await getAccount();
        if (cancelled) return;
        setAccount(record);
        setPhase("signed-in");
      } catch (error) {
        if (cancelled) return;
        // An unreachable or unconfigured API is not a signed-out reader, but
        // it leaves them with the same options, so the control offers them.
        // The reason is shown rather than swallowed: a 503 here means the
        // deployment has registered no OIDC client, and a reader pressing
        // "sign in" repeatedly deserves to know that.
        setFailure(apiErrorMessage(error));
        setPhase("signed-out");
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Once signed in, keep it that way. The access token lasts fifteen minutes
  // and a reader's session lasts thirty days; without this, every screen that
  // saves would fail on the reader's click a quarter of an hour after they
  // signed in.
  useEffect(() => {
    if (phase !== "signed-in") {
      return undefined;
    }
    return maintainSession();
  }, [phase]);

  const begin = useCallback(async () => {
    setFailure("");
    setPhase("working");
    try {
      const destination = await startSignIn();
      // A full navigation rather than a router push: the destination is the
      // provider's origin, and the router does not go there.
      window.location.assign(destination);
    } catch (error) {
      setFailure(apiErrorMessage(error));
      setPhase("signed-out");
    }
  }, []);

  const end = useCallback(async () => {
    setFailure("");
    setPhase("working");
    try {
      await signOut();
    } catch (error) {
      setFailure(apiErrorMessage(error));
    } finally {
      setAccount(null);
      setPhase("signed-out");
    }
  }, []);

  if (phase === "checking" || !resolved) {
    return (
      <span className="sign-in-control" data-state="checking">
        <span aria-live="polite">Checking sign-in…</span>
      </span>
    );
  }

  if (phase === "signed-in") {
    const label = account?.public_display_name || account?.email || "Account";
    return (
      <span className="sign-in-control" data-state="signed-in">
        <span className="sign-in-identity" title={label}>
          {label}
        </span>
        <button type="button" onClick={end} className="nav-link">
          Sign out
        </button>
      </span>
    );
  }

  // Signed out, or working. An operator token pasted on the saved-analysis
  // screen is a credential too, and saying so here stops the control from
  // contradicting a screen that is happily writing to an account.
  const operator = heldCredentialKind() === "operator";
  return (
    <span className="sign-in-control" data-state="signed-out">
      {operator ? <span className="sign-in-identity">Operator token</span> : null}
      <button
        type="button"
        onClick={begin}
        className="nav-link"
        disabled={phase === "working"}
      >
        {phase === "working" ? "Signing in…" : "Sign in"}
      </button>
      {failure ? (
        <span className="sign-in-failure" role="status">
          {failure}
        </span>
      ) : null}
    </span>
  );
}
