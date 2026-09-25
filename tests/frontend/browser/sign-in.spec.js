import { expect, test } from "../support/servedRequests.js";

// Covers: WEB-116 — the sign-in surface in a real browser. The authorization
// code leaves the address bar before anything can read it, a session is
// recovered across a reload from a cookie the page cannot see, the control
// says something true in each of its three states, and a refused sign-in is
// reported without repeating the API's own stable refusal twice.
//
// The provider is not reachable from here and should not be: what this tier
// can grade is everything on this side of the redirect, which is all of it
// except the provider's own consent screen.

const ACCOUNT = {
  public_display_name: null,
  email: "reader@example.test",
  created_at: "2026-09-01T00:00:00Z",
};

/**
 * Install the identity routes.
 *
 * Every page in this application renders the sign-in control, so
 * `/auth/refresh` is called on every navigation. A spec that stubbed it only
 * where it was the subject would get a real request against a server with no
 * API behind it, and the failure would land in whichever test happened to
 * navigate first.
 */
async function installIdentity(page, { signedIn = false, calls = [], overrides = {} } = {}) {
  await page.route("**/api/v1/auth/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace("/api/v1/auth/", "");
    let body = null;
    try {
      body = request.postDataJSON();
    } catch {
      body = null;
    }
    calls.push({ path, method: request.method(), body, url: request.url() });

    if (overrides[path]) {
      return overrides[path](route, body);
    }
    if (path === "refresh") {
      return signedIn
        ? route.fulfill({ status: 200, json: session() })
        : route.fulfill({ status: 401, json: { detail: "sign-in could not be completed" } });
    }
    if (path === "sign-in") {
      return route.fulfill({
        status: 200,
        json: { authorization_url: "/auth/stub-provider" },
      });
    }
    if (path === "callback") {
      return route.fulfill({ status: 200, json: session() });
    }
    if (path === "sign-out" || path === "sign-out-everywhere") {
      return route.fulfill({ status: 204, body: "" });
    }
    return route.fulfill({ status: 404, json: { detail: "not found" } });
  });

  await page.route("**/api/v1/account", async (route) =>
    route.fulfill({ status: 200, json: ACCOUNT }),
  );
}

function session() {
  return {
    access_token: "an-access-token",
    token_type: "Bearer",
    expires_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
    expires_in: 900,
  };
}

test("a visitor with no session is offered one", async ({ page }) => {
  await installIdentity(page, { signedIn: false });
  await page.goto("/");

  const control = page.locator(".sign-in-control");
  await expect(control).toHaveAttribute("data-state", "signed-out");
  await expect(control.getByRole("button", { name: "Sign in" })).toBeVisible();
});

test("a session is recovered across a reload from a cookie the page cannot read", async ({
  page,
}) => {
  // This is the whole of "stay signed in": the access token died with the
  // previous page, and the `HttpOnly` refresh cookie is the only thing that
  // survived it. Nothing in the page's own storage is involved, which is the
  // property ADR-0005 §2 chose this shape for.
  const calls = [];
  await installIdentity(page, { signedIn: true, calls });
  await page.goto("/");

  const control = page.locator(".sign-in-control");
  await expect(control).toHaveAttribute("data-state", "signed-in");
  await expect(control).toContainText("reader@example.test");
  await expect(control.getByRole("button", { name: "Sign out" })).toBeVisible();

  expect(calls.filter((call) => call.path === "refresh")).toHaveLength(1);
  expect(await page.evaluate(() => JSON.stringify(window.sessionStorage))).not.toContain(
    "an-access-token",
  );
  expect(await page.evaluate(() => JSON.stringify(window.localStorage))).not.toContain(
    "an-access-token",
  );
});

test("starting a sign-in sends the callback URL and navigates away", async ({ page }) => {
  const calls = [];
  await installIdentity(page, { signedIn: false, calls });
  await page.goto("/");

  await page.locator(".sign-in-control").getByRole("button", { name: "Sign in" }).click();
  await page.waitForURL("**/auth/stub-provider");

  const started = calls.find((call) => call.path === "sign-in");
  expect(started.method).toBe("POST");
  // Built from the live origin rather than configured, so a deployment cannot
  // hold a value that disagrees with where it is actually served.
  expect(started.body.redirect_uri).toBe(`${new URL(page.url()).origin}/auth/callback`);
});

test("the authorization code leaves the address bar and travels in a body", async ({
  page,
}) => {
  const calls = [];
  await installIdentity(page, { signedIn: false, calls });

  await page.goto("/auth/callback?code=4%2Fan-authorization-code&state=the-state");
  await page.waitForURL("**/saved");

  // The address bar, the history entry, and anything that reads
  // `document.referrer` on the next navigation.
  expect(page.url()).not.toContain("code=");
  expect(page.url()).not.toContain("4/an-authorization-code");

  const completed = calls.find((call) => call.path === "callback");
  expect(completed.method).toBe("POST");
  expect(completed.body).toEqual({
    code: "4/an-authorization-code",
    state: "the-state",
  });
  // Not in the request URL either: the body is the only place it appears.
  expect(completed.url).not.toContain("an-authorization-code");
});

test("a refused sign-in still clears the code, and says so once", async ({ page }) => {
  const calls = [];
  await installIdentity(page, {
    signedIn: false,
    calls,
    overrides: {
      callback: (route) =>
        route.fulfill({
          status: 401,
          json: { detail: "sign-in could not be completed" },
        }),
    },
  });

  await page.goto("/auth/callback?code=4%2Fa-spent-code&state=the-state");
  await expect(page.getByRole("heading", { name: "Sign-in did not complete" })).toBeVisible();

  // The failure path is exactly when a reader reloads, screenshots, or pastes
  // the URL into a support request, so it is the path that most needs the code
  // gone.
  expect(page.url()).not.toContain("code=");

  const body = await page.locator("body").innerText();
  // The API answers one stable refusal for every way a sign-in can fail;
  // repeating it verbatim would tell a reader the same sentence twice.
  expect(body.match(/could not be completed/g) || []).toHaveLength(1);
});

test("a provider that refused is reported as itself", async ({ page }) => {
  const calls = [];
  await installIdentity(page, { signedIn: false, calls });

  await page.goto("/auth/callback?error=access_denied");
  await expect(page.getByRole("heading", { name: "Sign-in did not complete" })).toBeVisible();
  await expect(page.locator("body")).toContainText("access_denied");

  // Nothing was exchanged: there was no code to exchange.
  expect(calls.filter((call) => call.path === "callback")).toHaveLength(0);
});

test("signing out returns the control to its signed-out state", async ({ page }) => {
  const calls = [];
  await installIdentity(page, { signedIn: true, calls });
  await page.goto("/");

  const control = page.locator(".sign-in-control");
  await expect(control).toHaveAttribute("data-state", "signed-in");
  await control.getByRole("button", { name: "Sign out" }).click();

  await expect(control).toHaveAttribute("data-state", "signed-out");
  const signedOut = calls.find((call) => call.path === "sign-out");
  expect(signedOut.method).toBe("POST");
});
