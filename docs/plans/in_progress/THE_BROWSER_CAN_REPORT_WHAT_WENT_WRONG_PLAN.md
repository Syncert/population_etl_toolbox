---
id: client-error-and-vitals-reporting
branch: claude/client-error-and-vitals-reporting
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build ; npm --prefix apps/web run check:csp
  - npm --prefix apps/web run test:browser
---

# The browser can report what went wrong

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit.
  **Decision taken 2026-09-16 by the repository owner: the sink is a Next
  route handler in the web app.** No API change is needed by this plan.
- **Status:** Deliverables 1 and 2 are implemented on
  `claude/plans-folder-iteration-4x6itr` and every tier a cloud session can
  run is green. **Deliverable 3 is not done, and not merely unverified:**
  there is no CI job that runs the `web` container at all, so there is no
  container log to assert against. What that deliverable actually needs is
  written out below; it is a larger change than "add a grep to a workflow",
  and inventing it here without a Docker daemon to check it against would be
  a workflow edit nobody has run.
- **Last updated:** 2026-09-17
- **Current milestone:** deliverable 3, on a machine.

## Why

`docs/plans/completed/DEPLOYMENT_OBSERVABILITY_PLAN.md` says plainly that
"the frontend cannot be debugged" and delivered server-side smoke tiers in
response. Nothing since has given the browser a voice: `useReportWebVitals`,
`window.onerror`, `unhandledrejection` and `securitypolicyviolation` appear
nowhere under `apps/web/{app,components,lib}`, and `apps/web/middleware.ts`
sets no `report-to` or `report-uri` on the Content-Security-Policy. The CSP
browser spec proves zero violations in CI on a build CI just made; a
violation in production, a chunk blocked under the nonce policy, a MapLibre
WebGL failure or a hydration mismatch produces no signal anywhere.

The CSP fixes the sink: `connect-src 'self'` (`middleware.ts`), so a report
must go to the same origin. `next.config.mjs` rewrites `/api/*` and
`/tiles/*` to the API and Martin, so a Next route handler must sit
elsewhere.

## Deliverables

### 1. The sink

A Next route handler at a path outside the rewritten prefixes (for example
`/_report`) that validates a small, closed report shape, writes one
structured line to the web container's stdout, and answers `204`. It holds
no state, accepts only `POST` with a bounded body, and answers `204` to an
invalid report as well (a reporter must never retry into a sink). The
handler is same-origin, so `connect-src 'self'` admits it without any CSP
change.

### 2. Three reporters, one payload discipline

`useReportWebVitals` in the layout; a `window.onerror`/`unhandledrejection`
hook registered once; `report-to` (and `report-uri` for older agents) on
the CSP pointing at the sink. Every payload carries route path (not query),
metric or error name, message, and the app's build id, and never a token,
a query string, a value, a saved-analysis name or an id.

### 3. The deployment reads it

The Compose smoke job asserts that the web container log contains one
vitals line after the browser tier's first navigation, so the path is
proven on every push.

## Acceptance criteria

- [x] A unit test asserts the payload builder strips query strings and
      refuses any field outside the closed shape -- field by field, including
      a token, a query, a saved-analysis name, an identifier, and a
      measurement on a kind that measures nothing.
- [x] A browser test triggers a synthetic error and a synthetic CSP violation
      and asserts one request each to the sink, with the payload shape on the
      wire and no query string in any report.
- [x] `check:csp` passes with the `report-to` directive present, and the
      budget check passes with the sink's own route budget declared.
- [ ] The Compose smoke job sees the vitals line in the web container log.
      **Not done.** No CI job runs the `web` container; see below.
- [x] `TESTING_CONTRACT.md` gains WEB-114. The `DEPLOY-` row belongs with
      deliverable 3 and is not written, because a row describing a check that
      does not run is the thing this catalog exists to prevent.

## Implementation evidence

### The shape is closed, and closed is the point

`lib/clientReport.ts` holds all of it: what a report may say, how each of the
three reporters builds one, and how it is sent. A field outside the shape
makes the whole report `null` rather than being stripped, because "stripped"
is how a field nobody meant to send starts arriving. The route is reduced to
a path by the builder and *again* by the sink, which does not trust the
reporters -- anything can POST to a same-origin path.

One deliberate departure from the plan's list. Deliverable 2 says a payload
carries "route path, metric or error name, message, and the app's build id",
and forbids "a value". A Web Vital without its measurement reports nothing at
all, so a `vital` carries one and every other kind is refused if it tries to.
The forbidden "value" is an observed one -- something drawn from the data --
and a vitals number is a measurement of this application's rendering. If a
reviewer disagrees, deleting the `value` branch in `parseReport` removes it
everywhere, and the tests that assert it will say so.

### The component is four lines

`ClientReporters.tsx` registers three listeners and a hook and renders
nothing. The logic lives in the library on purpose: a component holding it
could only be tested by rendering it, which means mocking `next/navigation`
and `next/web-vitals` -- and a test that mocks the framework proves what the
mock does. It was written that way first, and the mocks did not even resolve
from the tests' directory, which was the nudge to do it properly.

The route comes from `usePathname`, not `location.href`: the pathname cannot
leak a query string because it never holds one.

### The unit tier is where the vitals path is provable

A Web Vital is reported when a page is hidden or unloaded, which a browser
test can only cause by ending itself. `sendReport` takes an injectable
transport -- the same shape `apiFetch` uses for `fetchImpl` -- so the beacon
and the `keepalive` fetch fallback are both asserted without a browser.

The sink is unit-tested by importing the route handler and calling it. That
is the only tier where its *output* is observable at all: Playwright runs its
web server with stdout ignored, and a container log needs Docker. It answers
`204` to everything, writes nothing for a report it refuses, and cannot be
forged into two log lines -- a message carrying a newline and a plausible
second record comes out as one line with the forgery inside a JSON string.

### What the browser tier could not read, and what was done about it

Playwright cannot read the body of a `sendBeacon` request: Chromium does not
expose a `Blob` payload to the protocol. A spec asserting only "a request was
made" would pass just as well on a report carrying a bearer token, which is
the opposite of what this spec is for. So two tests take `sendBeacon` away in
an init script and read the `fetch` bodies, and a third leaves it in place and
proves the beacon path a real reader's browser takes reaches the sink.

### A real cost, found by measuring

The reporters make the Next **development** server log
`uncaughtException: [Error: aborted] ECONNRESET` once per beacon that a page
close aborts: 68 lines in a full browser run, where earlier runs in this
session had zero. It was worth pinning down rather than living with, so it
was measured three ways:

- With the reporters registered: 24 lines in one spec. With the component
  commented out: 0.
- With the sink returning `204` before reading the body at all: 28 lines. So
  it is not the body read -- it is the aborted connection itself.
- Against `next start`, which is what CI runs and what the deployed container
  runs: **0 lines**, and the full tier is green.

So it is a `next dev` behaviour, it does not reach the log this feature
writes to, and it is not something application code can prevent -- the error
is raised below the route handler. It is recorded here rather than left for
someone to rediscover.

Two of this session's own browser assertions had to change because of it:
`bundle-split.spec.js` waited for an idle network, and an application that
reports its own vitals has periodic traffic by design. They wait on the state
they are about instead, which is what they should have done in the first
place.

### What deliverable 3 actually needs

The plan says "the Compose smoke job asserts that the web container log
contains one vitals line". Nothing in CI runs the web container:
`docker-compose.smoke.yml` defines `postgres`, `api` and `proxy` only, and
`frontend-smoke` starts `postgres martin api proxy`. The `web` service exists
in `docker-compose.yml`, the deployment stack, which no workflow brings up.

And a vitals line needs a *browser*: `curl` against the web container
produces none. So the deliverable is really three changes, and they should be
made where they can be run:

1. Add the `web` service to the smoke stack (it already builds from
   `Dockerfile.web`).
2. Point a browser at it -- either the Playwright tier with its base URL set
   to the container, or one scripted navigation.
3. Grep `docker compose logs web` for `client_report kind=vital`.

A machine session can do all three and see them pass. Doing it here would
mean pushing a workflow nobody has run, against a stack this container cannot
start, which is the failure mode the execution-environment split exists to
prevent.

### Commands

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 639 passed, 43 files (was 603, 40) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:csp` / `check:bundle` | pass; the sink's route needed a budget, which the check demanded by name |
| `npm --prefix apps/web run test:browser` (`next start`, as CI runs it) | 155 passed, 0 failed, 0 uncaught exceptions |
| `npm --prefix apps/web run test:browser` (`next dev`, as a contributor runs it) | 155 passed, 0 failed, 68 dev-server abort lines |

Both guards were verified to fail without what they guard: with
`<ClientReporters />` removed from the layout, three of the five browser
tests fail; with `report-uri`/`report-to` removed from the policy, the
fourth does.

## Definition of done

A production regression a reader sees is a line an operator can read, and
the line carries nothing the privacy boundary keeps off the wire.

## What this plan deliberately does not do

- It does not add a third-party error service or any external endpoint; the
  CSP forbids it and the plan does not widen the CSP.
- It does not sample, batch or persist reports; stdout is the store.
- It does not rate-limit the sink. It writes one bounded line per accepted
  report and holds no state, so the cost of a flood is log volume rather than
  memory or a database; a deployment that wants a bound has a proxy in front
  of it. Worth revisiting if this is ever exposed without one.
