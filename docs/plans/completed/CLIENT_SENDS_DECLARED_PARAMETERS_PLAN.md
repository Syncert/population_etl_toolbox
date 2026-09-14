---
id: client-sends-declared-parameters
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - strict-query-parameters
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:browser
  - npm --prefix apps/web run test:unit
---

# Every request the web client sends names parameters the API declares

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/frontend/support/`, `tests/frontend/browser/`

## Context

API-093 made the API refuse a query parameter the matched route does not
declare, because a misspelling used to be answered with a confident, wrong
page. That is a good rule for callers and a sharp edge for this one: a
request the web application sends today with a name the API does not declare
was a silently broader answer, and is now a 422 the user sees.

Nothing in this repository checks what the client actually sends. WEB-043
comes close and stops one step short: it reads the reviewed OpenAPI snapshot
so each browser fixture *declares* the parameters its routes accept, which
decides what the client is *allowed* to send. But the browser tier serves
every request from a `page.route` stub, and a stub answers whatever arrives.
A request carrying an undeclared name is fulfilled exactly like one that does
not.

So the tier that drives the real client against real URLs never looks at the
URLs. The check is cheap and the evidence is already flowing past:
`page.on("request")` observes every request the page makes, intercepted or
not, and the snapshot already says what each operation accepts.

This also corrects a claim in the API-093 plan, which recorded the browser
tier as evidence that "nothing the web application sends is refused". It is
not: those 71 tests would pass against an API that refuses every request the
application makes, because none of them reaches an API.

## Acceptance criteria

1. Every request the browser tier's pages make to an `/api/v1` path is
   checked against the reviewed snapshot: each query parameter must be one
   that operation declares.
2. A request to a path the snapshot does not serve at all is a failure too —
   a client calling a route nobody runs is the same defect one step earlier.
3. The check runs for every browser spec without each spec opting in, and a
   spec added later inherits it.
4. The names come from `tests/fixtures/api/openapi_contract.json`, never from
   a list written beside the tests.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Checking request bodies. `POST`/`PUT` payloads are validated by their
  models on the API side and by the fixtures here; this is about the query
  string API-093 now refuses.
- Checking requests from pages a spec opens itself rather than the `page`
  fixture. The guard follows the fixture, which is what every current spec
  drives.
- Making the frontend stubs reject an undeclared parameter. Failing the test
  with the offending URL is more useful than a 422 the app then renders as a
  generic error.

## Validation

**The guard finds nothing, and the finding-nothing is checked.** The browser
tier passes 71/71 with the fixture installed, which is only worth something
if the fixture can fail. Adding one undeclared parameter to the catalog
client's request parameters —

```diff
   const params: Record<string, string> = {
+    source_codes: "TEMPORARY",
     limit: String(pageSize),
```

— fails every catalog spec with the URL and the remedy in the message:

```
Error: the client sent parameters the API does not declare
+ "/api/v1/catalog/metrics was sent source_codes, which
   GET /api/v1/catalog/metrics does not declare;
   it accepts active_only, limit, offset, q, source_code"
```

The temporary parameter was reverted; `git diff apps/web/lib/catalog.ts` is
empty.

**Where it lives.** `requestComplaint` is in
`tests/frontend/support/servedContract.js`, beside the snapshot it reads and
with no Playwright import, so the unit tier grades it directly: a declared
request, an undeclared name, a templated path (`/catalog/metrics/{metric_code}`),
an unserved path, and a non-API request. `tests/frontend/support/servedRequests.js`
is the thin Playwright layer: an automatic fixture, so the eleven specs each
changed one import line and a spec added later inherits the check.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | 280 passed (was 275) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 71 passed |
| Frontend lint | `npm --prefix apps/web run lint` | clean |
| Unit | `pytest tests/unit` | 1449 passed |

**Register.** 340 rows.

**API-093's plan corrected.** It recorded the browser tier as evidence that
nothing the web application sends is refused. That tier answers every request
from a stub, so it would have passed against an API that refused all of them.
The claim is replaced with what is actually true, and with a pointer to this
row as the guard that checks it.

## Remaining work

- None.

## The inheritance claim, made true at review (2026-09-14)

Criterion 3's second clause — "a spec added later inherits it" — and the same
sentence in the WEB-052 row were true only by convention. The guard rides on
the `test` object each spec imports from `tests/frontend/support/servedRequests.js`;
a new spec importing `test` from `@playwright/test` instead got a plain
fixture, sent whatever it liked, and nothing failed. Twelve specs import it
correctly, so nothing was wrong — but nothing was holding it, either.

`tests/unit/shared/test_repository_hygiene.py::test_every_browser_spec_inherits_the_served_request_guard`
now reads every `tests/frontend/browser/*.spec.js` and fails, naming the file,
when one imports from `@playwright/test` or from neither module. Verified by
adding a spec that imports from `@playwright/test`: the guard failed with that
file's name, and passed again once it was removed. It rides `unit etl`, which
is the tier that already owns the repository-hygiene guards.

