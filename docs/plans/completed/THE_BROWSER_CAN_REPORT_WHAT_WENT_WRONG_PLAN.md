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
- **Status:** Ready for review. Deliverable 3 was built and run on a machine
  session on 2026-09-18: the smoke stack now composes the `web` container, one
  Chromium navigation is driven at it, and the job greps that container's log
  for `client_report kind=vital`. The whole sequence was executed here, not
  just written. See "Deliverable 3, built and run".
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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
- [x] The Compose smoke job sees the vitals line in the web container log.
      Built and run on 2026-09-18; see "Deliverable 3, built and run".
- [x] `TESTING_CONTRACT.md` gains WEB-114, and DEPLOY-011 now that the check
      it describes runs. `CI_EVIDENCE_MAP.md` gains the matching row, and
      WEB-114's entry there no longer says no tier can see the deployed log.

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

### Deliverable 3, built and run

All three changes the section above called for were made and executed on a
Windows 11 machine with a Docker daemon on 2026-09-18.

**1. The smoke stack composes `web`.** It builds from `Dockerfile.web` with
`API_ORIGIN` and `TILES_ORIGIN` pointing at this stack's own services, and
carries a healthcheck written in `node` -- the image has no `curl` or `wget`,
and a healthcheck that shells out to a binary the image does not carry is a
container that never reports healthy and a `--wait` that times out with
nothing to read.

**2. One navigation, in `apps/web/scripts/report-a-vital.mjs`.** It drives
Chromium at the container, waits until the sink has *answered* a report, and
leaves.

The response is what it waits for rather than the request, and that is not a
detail: `sendReport` prefers `navigator.sendBeacon`, and a beacon is
fire-and-forget, so watching the request leave says nothing about whether the
container received it. A 204 is the sink's own answer and the sink logs the
line before answering, so once one has arrived the grep that follows is not
racing it.

**What it deliberately does not assert is the kind.** The first version
checked the request body for `"kind":"vital"` and reported "no vitals report
was sent" while the container's log held two of them. Playwright reports a
beacon's body as `null` on both `postData()` and `postDataBuffer()` -- the
body is not available on that side at all. Making it visible would mean
forcing the `fetch` fallback, which grades a transport production does not
use. So the script asserts that a report was accepted, and the *kind* is the
log's business, which is where the job asserts it. The split is worth keeping
for its own sake: a browser that never reported and a sink that never wrote
the line are different faults, and only the navigation can see the first.

**3. The job greps the log.** `frontend-smoke.yml` starts `web` with the rest
of the stack, installs the Chromium the navigation needs, runs it, and greps
`docker compose logs web` for `client_report kind=vital`. Run here against a
freshly restarted container:

```text
2 client report(s) accepted at /client-report
client_report kind=vital route=/ name=TTFB build=... value=19.7 message="good"
client_report kind=vital route=/ name=FCP  build=... value=92   message="good"
```

Next's own hydration and render metrics arrive as soon as the page settles,
so the navigation does not have to wait for the Core Web Vitals that fire on
page-hide.

`tests/unit/deployment/test_stack_configuration_contracts.py` guards the three
together, because each is useless alone: the stack composes the container, the
job starts it, and the job reads its log.

### A defect this turned up, fixed separately

The first line the deployed container logged read `build=development`.

`NEXT_PUBLIC_BUILD_ID` is inlined by `next build` -- a client bundle cannot
read the server's environment -- and `Dockerfile.web` never accepted it, so
**every image the repository builds reports `development`**, a deployment's
included. `next.config.mjs` states exactly what that costs beside the fallback:
"a report that cannot say which build produced it sends an operator to read the
wrong source."

It is fixed in its own commit rather than folded in here: the field is
WEB-114's, but a build argument and two env examples are not among this plan's
deliverables, and the completion gate is about the criteria above. It is
recorded here because this plan is why it was found -- deliverable 3 is the
first thing that ever read the deployed container's output.

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

Re-run on the machine session of 2026-09-18, with deliverable 3 built:

| Command | Result |
|---|---|
| `npm --prefix apps/web run test:unit` | 639 passed, 43 files |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `npm --prefix apps/web run build` / `check:csp` / `check:bundle` | pass |
| `python -m pytest tests/unit/deployment -q` | 60 passed (was 59; DEPLOY-011's guard) |
| the smoke stack's own sequence: `up --wait ... web`, `npm run report:vital`, `grep client_report kind=vital` | pass |

**The unit tier needed two fixes of its own before it could say that**, both
the tests' and neither this plan's, committed separately: a fixture label built
with the platform's path separator, and date assertions that are the UTC
renderings of their instants with no time zone pinned. Together they failed
this tier on any developer machine west of UTC running Windows -- which is to
say, on the machine that first ran it -- while passing on every CI runner.

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
