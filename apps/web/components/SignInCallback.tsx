"use client";

// Where the provider returns the browser (ADR-0005 §1).
//
// The single most important line in this file is the `replaceState`, and it
// runs before the network call rather than after it. The address bar is
// currently holding a live authorization code: it is in `window.location`, in
// the session history entry, and available to anything that reads
// `document.referrer` on the next navigation. Clearing it first means a slow
// or failed exchange does not leave it sitting there, which is exactly when a
// reader is most likely to reload, screenshot, or paste the URL into a support
// request.
//
// The code is single-use and the API refuses a replayed transaction, so what
// is being protected here is not the code's usefulness to an attacker five
// minutes from now. It is the habit: a credential in a URL is a credential in
// places nobody audited, and the cost of not putting it there is two lines.

import { useEffect, useRef, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";

import { completeSignIn } from "../lib/session";

type Phase = "completing" | "done" | "failed";

/** Where a reader lands after signing in. Their saved work is the point. */
const AFTER_SIGN_IN = "/saved";

export default function SignInCallback(): JSX.Element {
  const router = useRouter();
  const [phase, setPhase] = useState<Phase>("completing");
  const [failure, setFailure] = useState<string>("");
  // React runs effects twice in development's strict mode. The second run
  // would present an authorization code the first one has already spent, and
  // the API would correctly refuse it — turning a successful sign-in into a
  // failure message on every developer's machine.
  const started = useRef(false);

  useEffect(() => {
    if (started.current) {
      return;
    }
    started.current = true;

    const parameters = new URLSearchParams(window.location.search);
    const code = parameters.get("code") || "";
    const state = parameters.get("state") || "";
    const providerError = parameters.get("error") || "";

    // First, before anything can observe it.
    window.history.replaceState(null, "", window.location.pathname);

    if (providerError) {
      // The provider refused or the reader declined. Reported as itself: this
      // is the one failure that is not about this application at all, and
      // "sign-in could not be completed" would be a worse answer than
      // "access_denied".
      setFailure(providerError);
      setPhase("failed");
      return;
    }
    if (!code || !state) {
      setFailure("This sign-in link is incomplete.");
      setPhase("failed");
      return;
    }

    completeSignIn(code, state)
      .then(() => {
        setPhase("done");
        router.replace(AFTER_SIGN_IN);
      })
      .catch(() => {
        // Deliberately not `apiErrorMessage`. The API answers one stable
        // refusal for every way a sign-in can fail, and repeating it verbatim
        // would tell a reader "sign-in could not be completed" twice.
        setFailure("That sign-in could not be completed. Please try again.");
        setPhase("failed");
      });
  }, [router]);

  if (phase === "failed") {
    return (
      <main className="page-shell">
        <h1>Sign-in did not complete</h1>
        <p role="status">{failure}</p>
        <p>
          <Link href="/">Return to the home page</Link> and try again.
        </p>
      </main>
    );
  }

  return (
    <main className="page-shell">
      <h1>Signing you in</h1>
      <p role="status" aria-live="polite">
        {phase === "done" ? "Signed in. Taking you to your saved work…" : "One moment…"}
      </p>
    </main>
  );
}
