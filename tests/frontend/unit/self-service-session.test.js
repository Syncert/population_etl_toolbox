import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

// Covers: WEB-115 — the browser holds one credential in one place, the session
// half lives only in memory, and an expired access token rotates once rather
// than being presented.
//
// The question this file exists to answer is the one the plan flagged as a
// decision rather than a coding task. Scope item 4 requires `lib/apiToken.ts`
// to stay the one home of a browser-held credential; ADR-0005 §2 requires the
// access token to live "only in JavaScript memory", while `apiToken` today is
// `sessionStorage` by WEB-022. The resolution is one module with two backings,
// and these tests are what hold it to both halves: nothing writes a session
// token to storage, and no screen has to know which kind it holds.

import {
  TOKEN_SESSION_KEY,
  clearSessionCredential,
  clearStoredToken,
  heldCredentialKind,
  readOperatorToken,
  readSessionCredential,
  readStoredToken,
  setSessionCredential,
  storeToken,
  subscribeToCredential,
} from "../../../apps/web/lib/apiToken";
import {
  callbackUrl,
  completeSignIn,
  maintainSession,
  refreshSession,
  signOut,
  startSignIn,
  withFreshSession,
} from "../../../apps/web/lib/session";

const HOUR = 60 * 60 * 1000;

/** A transport that answers a queue of responses and records every call. */
function transport(responses) {
  const calls = [];
  const queue = [...responses];
  const fetchImpl = async (path, init = {}) => {
    calls.push({ path, init });
    const next = queue.shift();
    if (!next) {
      throw new Error(`no queued response for ${path}`);
    }
    return {
      ok: next.status < 400,
      status: next.status,
      headers: { get: () => null },
      json: async () => next.body ?? {},
    };
  };
  return { fetchImpl, calls };
}

beforeEach(() => {
  clearSessionCredential();
  window.sessionStorage.clear();
});

afterEach(() => {
  clearSessionCredential();
  window.sessionStorage.clear();
  vi.restoreAllMocks();
});

describe("where the credential lives", () => {
  test("a session token is never written to any storage the page survives", () => {
    setSessionCredential("a-session-access-token", Date.now() + HOUR);

    expect(readSessionCredential()).toBe("a-session-access-token");
    // The whole of ADR-0005 §2's improvement over the operator token: script
    // that compromises the page can act, but cannot lift the credential and
    // reuse it from somewhere else later.
    expect(window.sessionStorage.getItem(TOKEN_SESSION_KEY)).toBeNull();
    expect(window.localStorage.getItem(TOKEN_SESSION_KEY)).toBeNull();
    expect(JSON.stringify(window.sessionStorage)).not.toContain(
      "a-session-access-token",
    );
    expect(JSON.stringify(window.localStorage)).not.toContain(
      "a-session-access-token",
    );
  });

  test("an operator token still lives exactly where WEB-022 put it", () => {
    storeToken("an-operator-token");

    expect(window.sessionStorage.getItem(TOKEN_SESSION_KEY)).toBe("an-operator-token");
    expect(readOperatorToken()).toBe("an-operator-token");
    expect(readStoredToken()).toBe("an-operator-token");
    expect(heldCredentialKind()).toBe("operator");
  });

  test("a session wins over a pasted operator token", () => {
    storeToken("an-operator-token");
    setSessionCredential("a-session-access-token", Date.now() + HOUR);

    // Preferring the older, longer-lived credential would make signing in
    // appear to do nothing to somebody who had pasted a token earlier.
    expect(readStoredToken()).toBe("a-session-access-token");
    expect(heldCredentialKind()).toBe("session");
  });

  test("signing out falls back to the operator token rather than to nothing", () => {
    storeToken("an-operator-token");
    setSessionCredential("a-session-access-token", Date.now() + HOUR);
    clearSessionCredential();

    expect(readStoredToken()).toBe("an-operator-token");
    expect(heldCredentialKind()).toBe("operator");
  });

  test("an expired session token is not returned", () => {
    setSessionCredential("an-expired-token", Date.now() - 1);

    // Presenting it would spend a request to learn what its own `expires_at`
    // already said, and the caller's next move is a rotation either way.
    expect(readSessionCredential()).toBe("");
    expect(readStoredToken()).toBe("");
    expect(heldCredentialKind()).toBe("none");
  });

  test("with no credential at all, there is nothing to report", () => {
    expect(readStoredToken()).toBe("");
    expect(heldCredentialKind()).toBe("none");
  });

  test("a credential change notifies every subscriber", () => {
    // Without this the five screens that authenticate would each keep
    // rendering whatever they read on mount, so a reader would sign in and
    // watch the page go on saying they had not.
    const seen = [];
    const stop = subscribeToCredential(() => seen.push(readStoredToken()));

    setSessionCredential("first", Date.now() + HOUR);
    clearSessionCredential();
    storeToken("operator");
    clearStoredToken();
    stop();
    setSessionCredential("after-unsubscribing", Date.now() + HOUR);

    expect(seen).toEqual(["first", "", "operator", ""]);
  });

  test("a browser with storage blocked reads empty rather than throwing", () => {
    const blocked = vi
      .spyOn(window.sessionStorage.__proto__, "getItem")
      .mockImplementation(() => {
        throw new DOMException("denied", "SecurityError");
      });

    expect(() => readOperatorToken()).not.toThrow();
    expect(readOperatorToken()).toBe("");
    blocked.mockRestore();
  });
});

describe("the sign-in flow", () => {
  test("the callback URL is built from the live origin", () => {
    // Built rather than configured, so a deployment cannot hold a value that
    // disagrees with where it is actually served. The API checks it against an
    // exact-match allowlist regardless.
    expect(callbackUrl("https://example.test")).toBe(
      "https://example.test/auth/callback",
    );
  });

  test("starting a sign-in posts the redirect URI and returns the destination", async () => {
    const { fetchImpl, calls } = transport([
      { status: 200, body: { authorization_url: "https://provider.test/authorize?x=1" } },
    ]);

    const destination = await startSignIn("https://example.test/auth/callback", {
      fetchImpl,
    });

    expect(destination).toBe("https://provider.test/authorize?x=1");
    expect(calls[0].path).toBe("/api/v1/auth/sign-in");
    expect(calls[0].init.method).toBe("POST");
    expect(JSON.parse(calls[0].init.body)).toEqual({
      redirect_uri: "https://example.test/auth/callback",
    });
  });

  test("completing a sign-in sends the code in a body, never in the path", async () => {
    const { fetchImpl, calls } = transport([
      {
        status: 200,
        body: {
          access_token: "the-access-token",
          token_type: "Bearer",
          expires_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
          expires_in: 900,
        },
      },
    ]);

    await completeSignIn("4/an-authorization-code", "the-state", { fetchImpl });

    // A code in a URL travels into history, into a `Referer`, and into
    // whatever a reader pastes when they ask for help.
    expect(calls[0].path).toBe("/api/v1/auth/callback");
    expect(calls[0].path).not.toContain("4/an-authorization-code");
    expect(JSON.parse(calls[0].init.body)).toEqual({
      code: "4/an-authorization-code",
      state: "the-state",
    });
    expect(readSessionCredential()).toBe("the-access-token");
  });

  test("the expiry is the API's answer, not a duration added to now", async () => {
    const expiresAt = new Date(Date.now() + 15 * 60 * 1000);
    const { fetchImpl } = transport([
      {
        status: 200,
        body: {
          access_token: "t",
          token_type: "Bearer",
          expires_at: expiresAt.toISOString(),
          // Deliberately disagreeing: a client that trusted this would believe
          // in a token for an hour after the API stopped accepting it.
          expires_in: 3600,
        },
      },
    ]);

    await completeSignIn("c", "s", { fetchImpl });

    expect(readSessionCredential(expiresAt.getTime() - 1000)).toBe("t");
    expect(readSessionCredential(expiresAt.getTime() + 1000)).toBe("");
  });

  test("a refusal at the callback holds no credential", async () => {
    const { fetchImpl } = transport([
      { status: 401, body: { detail: "sign-in could not be completed" } },
    ]);

    await expect(completeSignIn("c", "s", { fetchImpl })).rejects.toThrow();
    expect(readSessionCredential()).toBe("");
    expect(heldCredentialKind()).toBe("none");
  });
});

describe("rotation", () => {
  test("a rotation with no session answers false rather than throwing", async () => {
    // "Not signed in" is the ordinary state of most visitors, and the control
    // that calls this on mount should not have to catch an exception for it.
    const { fetchImpl } = transport([{ status: 401, body: { detail: "refused" } }]);

    await expect(refreshSession({ fetchImpl })).resolves.toBe(false);
  });

  test("a rotation sends no credential of its own", async () => {
    const { fetchImpl, calls } = transport([
      {
        status: 200,
        body: {
          access_token: "rotated",
          token_type: "Bearer",
          expires_at: new Date(Date.now() + HOUR).toISOString(),
          expires_in: 900,
        },
      },
    ]);

    await refreshSession({ fetchImpl });

    // The refresh cookie is the credential, and it is `HttpOnly`: the browser
    // attaches it to this one path and script cannot read it. An
    // `Authorization` header here would mean something else was being used.
    expect(calls[0].init.headers.Authorization).toBeUndefined();
    expect(readSessionCredential()).toBe("rotated");
  });

  test("an expired token rotates once and the call is retried", async () => {
    setSessionCredential("expired", Date.now() - 1);
    const { fetchImpl, calls } = transport([
      {
        status: 200,
        body: {
          access_token: "rotated",
          token_type: "Bearer",
          expires_at: new Date(Date.now() + HOUR).toISOString(),
          expires_in: 900,
        },
      },
    ]);

    const presented = [];
    const result = await withFreshSession(
      async (token) => {
        presented.push(token);
        return "done";
      },
      { fetchImpl },
    );

    expect(result).toBe("done");
    expect(presented).toEqual(["rotated"]);
    expect(calls).toHaveLength(1);
  });

  test("a 401 mid-flight rotates once and retries once, and no more", async () => {
    setSessionCredential("live-but-rejected", Date.now() + HOUR);
    const { fetchImpl } = transport([
      {
        status: 200,
        body: {
          access_token: "rotated",
          token_type: "Bearer",
          expires_at: new Date(Date.now() + HOUR).toISOString(),
          expires_in: 900,
        },
      },
    ]);

    let attempts = 0;
    const run = async () => {
      attempts += 1;
      const error = new Error("unauthorized");
      error.name = "ApiError";
      // The real `ApiError`, via a refused call, so the retry path is reached
      // by the same instanceof check the implementation uses.
      const { ApiError } = await import("../../../apps/web/lib/api/client");
      throw new ApiError({ status: 401, detail: "refused", path: "/api/v1/x" });
    };

    await expect(withFreshSession(run, { fetchImpl })).rejects.toThrow();
    // Rotating and retrying more than once would turn a genuinely revoked
    // session into a loop against a deliberately rate-limited endpoint.
    expect(attempts).toBe(2);
  });

  test("a failure that is not a 401 is not retried", async () => {
    setSessionCredential("live", Date.now() + HOUR);
    const { fetchImpl, calls } = transport([]);

    let attempts = 0;
    const run = async () => {
      attempts += 1;
      const { ApiError } = await import("../../../apps/web/lib/api/client");
      throw new ApiError({ status: 503, detail: "unavailable", path: "/api/v1/x" });
    };

    await expect(withFreshSession(run, { fetchImpl })).rejects.toThrow();
    expect(attempts).toBe(1);
    expect(calls).toHaveLength(0);
  });
});

describe("signing out", () => {
  test("the credential is dropped even when the call fails", async () => {
    setSessionCredential("held", Date.now() + HOUR);
    const { fetchImpl } = transport([{ status: 503, body: { detail: "down" } }]);

    // A sign-out that left the credential held because the network was down
    // would show a reader as signed in when they have asked not to be.
    await expect(signOut({ fetchImpl })).rejects.toThrow();
    expect(readSessionCredential()).toBe("");
  });

  test("signing out presents the token it is ending", async () => {
    setSessionCredential("held", Date.now() + HOUR);
    const { fetchImpl, calls } = transport([{ status: 204, body: {} }]);

    await signOut({ fetchImpl });

    expect(calls[0].path).toBe("/api/v1/auth/sign-out");
    expect(calls[0].init.headers.Authorization).toBe("Bearer held");
    expect(readSessionCredential()).toBe("");
  });

  test("signing out with nothing held reaches the API not at all", async () => {
    const { fetchImpl, calls } = transport([]);

    await signOut({ fetchImpl });

    expect(calls).toHaveLength(0);
  });
});


describe("keeping a session alive", () => {
  test("a rotation is scheduled before the token expires, not after", async () => {
    vi.useFakeTimers();
    try {
      const expiresAt = Date.now() + 15 * 60 * 1000;
      setSessionCredential("first", expiresAt);
      const { fetchImpl, calls } = transport([
        {
          status: 200,
          body: {
            access_token: "rotated",
            token_type: "Bearer",
            expires_at: new Date(expiresAt + 15 * 60 * 1000).toISOString(),
            expires_in: 900,
          },
        },
      ]);

      const cancel = maintainSession(() => {}, { fetchImpl });

      // A minute before expiry, not a moment after: waiting for the 401 works
      // but spends the reader's click.
      await vi.advanceTimersByTimeAsync(14 * 60 * 1000 - 1);
      expect(calls).toHaveLength(0);
      await vi.advanceTimersByTimeAsync(2);
      expect(calls).toHaveLength(1);
      expect(readSessionCredential()).toBe("rotated");
      cancel();
    } finally {
      vi.useRealTimers();
    }
  });

  test("cancelling stops the next rotation", async () => {
    vi.useFakeTimers();
    try {
      setSessionCredential("held", Date.now() + 15 * 60 * 1000);
      const { fetchImpl, calls } = transport([]);
      const cancel = maintainSession(() => {}, { fetchImpl });
      cancel();
      await vi.advanceTimersByTimeAsync(60 * 60 * 1000);
      expect(calls).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });

  test("a session that stops moving forward stops being rotated", async () => {
    // A deployment whose access-token lifetime is shorter than the rotation
    // window would compute a zero delay every time. Without a guard that is a
    // tight loop against the most rate-limited endpoint on the API.
    vi.useFakeTimers();
    try {
      const frozen = Date.now() + 10_000;
      setSessionCredential("short", frozen);
      const { fetchImpl, calls } = transport(
        Array.from({ length: 50 }, () => ({
          status: 200,
          body: {
            access_token: "short",
            token_type: "Bearer",
            expires_at: new Date(frozen).toISOString(),
            expires_in: 10,
          },
        })),
      );

      const cancel = maintainSession(() => {}, { fetchImpl });
      await vi.advanceTimersByTimeAsync(60 * 1000);
      expect(calls.length).toBeLessThanOrEqual(1);
      cancel();
    } finally {
      vi.useRealTimers();
    }
  });

  test("nothing is scheduled when no session is held", () => {
    vi.useFakeTimers();
    try {
      const { fetchImpl, calls } = transport([]);
      maintainSession(() => {}, { fetchImpl });
      vi.advanceTimersByTime(60 * 60 * 1000);
      expect(calls).toHaveLength(0);
    } finally {
      vi.useRealTimers();
    }
  });
});
