---
id: two-shapes-of-a-refused-request
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - npm --prefix apps/web run test:unit
---

# The two shapes of a refused request, said once

## Plan status

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Claimed and completed 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `docs/reference/API_CONSUMER_GUIDE.md`,
  `apps/web/lib/api/client.ts`

## Context

The guide's opening says everything in it is "pinned by the reviewed OpenAPI
snapshot … so a change to anything below appears in review as a snapshot
diff". Its Errors section then says:

> Every error body is `{"detail": "..."}`.

The snapshot says otherwise, for the one status the same section calls "a
request the API can explain":

```json
"HTTPValidationError": { "properties": { "detail": "array<ValidationError>" } }
```

and every paged read declares `"422": "application/json:HTTPValidationError"`.
Read against the running application, the snapshot is right:

```
422 /api/v1/catalog/metrics?limit=5000
{"detail":[{"type":"less_than_equal","loc":["query","limit"],
            "msg":"Input should be less than or equal to 1000",
            "input":"5000","ctx":{"le":1000}}]}
```

So `422` has two bodies. A refusal the API decides — an unsupported filter, a
contradictory scope, an incompatible comparison, a reversed range — raises
`HTTPException(422, detail="…")` and answers a string. A malformed or missing
parameter, or a body that fails schema validation, is refused by the
framework before the endpoint runs and answers the declared array. `413` and
`503` were checked and do answer a string, as documented.

The consequence is not hypothetical. `decodeErrorDetail` in the web client
reads the body defensively:

```ts
const detail = (payload as { detail?: unknown } | null)?.detail;
return typeof detail === "string" ? detail : null;
```

An array is therefore *correctly* refused as a string and dropped, and
`apiErrorMessage` renders `status 422` and nothing else. On the one status
class the guide says the API can explain, the client shows the reader a
number and discards the explanation it was handed — the same defect WEB-040
fixed for `Retry-After`, whose reason line reads "a sentence that points at a
number the client held on the error object and dropped on the way to the
screen".

## Considered and rejected

Making reality match the prose — a `RequestValidationError` handler that
renders the errors into one `{"detail": "…"}` string — was the other way to
remove the contradiction. Rejected: it would replace the `HTTPValidationError`
every OpenAPI-aware client already understands with a string that no longer
says *which* parameter was refused in a machine-readable way, and the served
`422` declaration would have to be overridden on every route or the snapshot
would contradict the prose in the opposite direction. The structured body is
the better contract; the prose is the half that is wrong.

## Acceptance criteria

1. The guide's Errors section states both `422` bodies, says which refusals
   answer which, and keeps the single-shape promise true of the statuses it
   is true of. The sentence that is wrong today does not survive.
2. A test reads both shapes off the served application rather than restating
   them: a malformed parameter answers an array whose entries carry `loc`,
   `msg` and `type`; a refusal the API explains answers a string. It fails if
   either shape changes, in either direction.
3. The guide's claim is derived from the reviewed snapshot too, so a route
   whose `422` declaration changed would fail rather than leave the prose
   standing.
4. The web client carries the structured explanation to the screen: which
   parameter was refused and why, for each entry, without echoing the
   caller's own submitted value back and without an unbounded message.
5. A body with no usable entry still degrades to the status line it shows
   today rather than an empty or malformed sentence.
6. Register rows: API-111 (the documented shapes) and WEB-063 (the client
   carrying them).

## Non-goals

- Changing any served response. Nothing about the API's behaviour is wrong
  here; the prose describing it and the client reading it are.
- Documenting the per-read page ceilings in prose. They differ (200 for the
  private stores, 1,000 for the catalog, releases and comparison, 5,000 for
  the observation reads) and the guide's pagination bullet does not name
  them, but the snapshot publishes every one as a parameter `maximum`, which
  is where a client reads bounds. No contradiction, so no change.

## What changed

- **The guide (API-111).** The blanket sentence is gone. The Errors table's
  `422` row now says a refusal comes from either side, and a paragraph below
  states both bodies with the `HTTPValidationError` shape shown as JSON,
  names `loc`/`msg`/`type` and what each is for, calls `input` and `ctx`
  diagnostic, and tells a client to read `detail`'s type before rendering it.
- **The guard (API-111).** `test_the_guide_describes_both_shapes_of_a_refused_request`
  reads both shapes off the running application: `limit=5000` on
  `/catalog/metrics` for the array (asserting its `loc` is
  `["query", "limit"]` and that every entry carries the keys the reviewed
  snapshot's own `ValidationError` marks required), and a reversed year
  window on `/observations` for the string. It then asserts the contract
  declares exactly one `422` shape across every operation, and that the
  retired sentence is absent while the section names each required key.
  The database dependency is overridden with a bare object: FastAPI resolves
  dependencies before the endpoint runs, and both refusals happen before the
  session is touched.
- **The client (WEB-063).** `describeValidationDetail` renders the array into
  `loc: msg` sentences joined by `"; "`, bounded at three entries with the
  remainder counted. `input` is deliberately not rendered. A body with no
  readable entry returns `null`, so the caller shows the status line it shows
  today.
- **A slip of my own, fixed in passing.** `tests/frontend/unit/api-client.test.js`
  imported `fetchAllPages` twice, introduced by WEB-056's commit (`b6c8723`).
  Duplicate imported bindings are an early `SyntaxError` per the language;
  esbuild tolerates it, so neither the tier nor eslint objected. Deduped.

## Validation

- `pytest tests/unit` — **1490 passed** (1489 before: the new guide node).
  `pytest tests/unit/shared` — 205 passed, register hygiene included.
- `npm --prefix apps/web run test:unit` — **334 passed** (331 before: +3
  client nodes).
- `npm --prefix apps/web run test:browser` — **80 passed**, unchanged: no
  fixture serves a validation body, so nothing a user sees moved.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `ruff format --check .` / `ruff check .` — clean (442 files).
- `python -m tests.support.catalog_evidence` renders API-111 and WEB-063
  `FULL`.

### Ground truth

Read off the application before any change was written, with
`DATABASE_URL` pointed at the local test database:

```
422 /api/v1/catalog/metrics?limit=5000   detail type: list
422 /api/v1/catalog/metrics?limit=abc    detail type: list
422 /api/v1/observations                 detail type: list   (missing metric_code)
422 /api/v1/observations?...&offset=999999  detail type: list
```

Every one carried `X-Request-ID`, as the guide promises. `413` and `503`
answer a string, as documented; with the database unconfigured the same
malformed requests answer `503` with a string, because the session
dependency is resolved before the parameters are validated — which is a
property of the deployment being unavailable, not of the refusal.

## Remaining work

- None. Review is the remaining step.
