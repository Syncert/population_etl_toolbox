---
id: served-contract-fixtures
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A frontend fixture cannot describe a contract the API does not serve

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13 after the same defect appeared twice while delivering WEB-036 and WEB-038.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/frontend/support/`, the frontend fixtures
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

The frontend suites stand a fake `/catalog/capabilities` in front of the
client, and each fake declares the query parameters its routes accept. The
client is built to send only declared parameters, so those lists decide what
every frontend test can observe. Nineteen of them name `/api/v1/observations`
across twelve files, and each is a hand-written copy.

Three of those copies say, in a comment, that they *are* the served list —
"Parameter lists are the served ones (see
tests/fixtures/api/openapi_contract.json)". Measured against that snapshot,
two of them were missing `newest_per_geography`, and a third was missing
`offset` on the time-series route until this branch corrected it.

A narrow fixture is legitimate and common: most of the nineteen deliberately
model a source that declares few filters, which is how the suite proves the
client never sends an undeclared one. The defect is narrower than "these
lists drift":

- a copy that **claims** to be the served list and is not silently models a
  weaker API than the one that ships, so the client's real behaviour goes
  untested; and
- a list naming a parameter the API does **not** serve would let a test
  prove the client sends something no deployment accepts.

The first cost two false starts on this branch: a test written against real
behaviour failed, and the failure was the fixture rather than the code. The
second has not happened — every one of the nineteen is currently a subset —
and nothing prevents it.

## Objective

The served list is read from the reviewed snapshot rather than copied, and no
fixture anywhere can name a parameter the API does not serve.

## Acceptance criteria

1. A shared support module exposes the query parameters the reviewed OpenAPI
   snapshot declares for a given `GET` path, and a helper that narrows that
   list explicitly.
2. The fixtures that claim to be the served list use it instead of a literal,
   so they cannot drift again.
3. Narrowing is explicit and bounded: a name that is not served cannot be
   removed from the list, because removing an unserved name is a typo rather
   than a narrowing.
4. A guard reads every `{path, parameters}` literal in the frontend fixtures
   and fails when one names a query parameter the snapshot's matching
   operation does not declare.
5. The guard is honest about what it cannot see: it names how many literals
   it examined, so a change to the fixture shape that stopped matching would
   be visible rather than passing vacuously.
6. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Rewriting the deliberately narrow fixtures. Their narrowness is the test.
- Generating capability fixtures wholesale from the snapshot; a capability
  response carries source identity and datasets the snapshot knows nothing
  about.
- Checking response *schemas* against the snapshot, which is a larger
  contract than the one that drifted.

## Evidence

### The gap, measured

The nineteen `/api/v1/observations` fixture lists were compared against the
snapshot before anything changed. None named a parameter the API does not
serve — the dangerous direction has not happened. Three of the three that
*claim* to be the served list were short: `explorer.spec.js` and
`explorer-sources.test.js` by `newest_per_geography`, and
`profiles.spec.js` by that and eight more behind a comment this branch had
just written.

The guard's own three tests then failed in a way worth recording: the
route-existence test reported eight unserved paths that turned out to be
per-source templates (`` `/api/v1/${segment}/observations/latest` ``), which
is why `servedPathsMatching` resolves a template against every served path of
that shape rather than skipping it. One genuinely unserved path remains and
is asserted by name: `explorer-sources.test.js` uses
`/api/v1/future/measures` to prove a source declaring only an unknown route
joins no explorer tab.

### What changed

- `tests/frontend/support/servedContract.js` reads the reviewed snapshot and
  exposes `servedParameters`, `servedParametersWithout`, `servedPathsMatching`,
  and `servesPath`. It finds the snapshot by walking up from the working
  directory rather than from `import.meta.url`: Playwright transpiles a `.js`
  spec and its imports as CommonJS, where `import.meta` is a syntax error,
  and both runners start from `apps/web`.
- The three fixtures that claim to be the served list now read it.
- `observation-access.test.js`'s deliberately reduced source is expressed as
  `servedParametersWithout("/api/v1/observations", ["newest_per_geography"])`,
  so the client's refusal is attributable to that one absence rather than to
  a fixture that is narrow in some other way.
- The guard reports how many literals and files it examined, with floors well
  under the current counts and far above zero, so a fixture-shape change that
  stopped matching is visible rather than silently vacuous.

The deliberately narrow fixtures were left alone. Their narrowness is the
test: it is how the suite proves the client never sends an undeclared
parameter.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 245 passed |
| `npm --prefix apps/web run test:browser` | 61 passed (Chromium) |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run build` / `check:bundle` | succeeded; every route within budget |
| `pytest tests/unit -q` | 1382 passed |
| `python -m tests.support.catalog_evidence` | 309-row register renders; WEB-043 is `FULL` |

One browser run reported `comparison.spec.js: a pair the declared policy
blocks is explained and never queried` as failed; the spec passed in
isolation and the full suite passed on one re-run, with no change in between.
Recorded rather than discarded.

### Not run

No API or smoke tier is implicated: this reads a checked-in snapshot and the
frontend fixtures, and runs entirely inside the `frontend` job that already
owns both.

## Remaining work

None.
