---
id: the-quality-screens-pointers-are-checked
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
---

# The quality screen's pointers are checked against the contract

## Plan status

- **Status:** Accepted 2026-09-14 (Implemented; awaiting review. Claimed and completed 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `tests/frontend/unit/data-quality.test.js`,
  `tests/frontend/support/servedContract.js`

## Context

`EVIDENCE_LOCATIONS` in `apps/web/lib/dataQuality.ts` is the data-quality
screen's answer to "where is the evidence for this kind of question", and its
docstring states the standard it holds itself to:

> A reader who cannot find a surface for one of them should be told where it
> really is, not shown a surface this client invented for it.

Each entry's `publishedBy` names API surfaces by name —
`/catalog/freshness`, `freshness_state`, `/observations/releases`,
`scope=as_released`, `value_status`, the `coverage` object,
`valid_geo_grains`, `valid_time_grains`, `publisher_contract_version`,
`source_watermark`. A reader follows those names.

The test that claims to check them is:

```js
test("each kind of quality evidence names its real publisher", () => {
  …
  for (const entry of EVIDENCE_LOCATIONS) {
    expect(entry.publishedBy).toBeTruthy();
```

**Truthiness is the whole check.** The node's name says "its real publisher"
and nothing crosses a single name against the reviewed contract, so a route
retired or a field renamed would leave the screen pointing a reader at a
surface that no longer exists, and this test would still pass. It is the
shape of the two guards WEB-051 and WEB-053 left unfailable, and of WEB-043's
own finding: "a fixture that models a weaker API than the one that ships does
not fail; it quietly stops testing the behaviour it names".

Read today, every name is real — this closes a hole rather than fixing a
break.

## Acceptance criteria

1. Every `/…` path an entry names is a served `GET` path of the reviewed
   contract, checked with the `/api/v1` prefix the guide's own prose omits.
2. Every snake_case name an entry uses appears in the reviewed contract —
   field names and parameter values alike, because `scope=as_released` names
   a value and `value_status` names a field, and a reader follows both.
3. Every entry names **at least one** published field or served path, so an
   entry of pure prose cannot pass the two checks above by naming nothing.
4. The node whose name over-claims is renamed to what it actually checks, so
   the suite stops asserting its own thoroughness.
5. The crossing reads the reviewed snapshot through
   `tests/frontend/support/servedContract.js`, the module that exists for
   exactly this, rather than a second copy of the contract.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-065).

## Non-goals

- Checking `inspectHere`, which names places in this application ("The
  explorer's Publication control"). A mechanical crossing would either be a
  string search over rendered labels — brittle and easy to satisfy
  accidentally — or a second description of the UI. The browser tier already
  asserts those controls exist by their own tests.
- Changing `EVIDENCE_LOCATIONS`. Nothing in it is wrong.

## What changed

- `tests/frontend/support/servedContract.js` gains `servedFieldNames()` (the
  union of every schema's property names — a reader following `coverage` or
  `value_status` does not care which envelope publishes it) and
  `servedContractWords()` (every snake_case word the document mentions, wider
  on purpose, because `as_released` is a value a parameter accepts rather
  than a field).
- `data-quality.test.js` gains the crossing, and the node that read
  `toBeTruthy()` under the name "names its real publisher" is renamed to
  "each kind of evidence is stated with what it is not", which is what it
  checks. The new node carries the old name and earns it.
- Nothing in `apps/web/lib/dataQuality.ts` changed: every name it gives is
  real today.

## Validation

- `npm --prefix apps/web run test:unit` — **348 passed** (347 before: +1).
- **The guard fails on a wrong claim.** Rewriting one entry's `publishedBy`
  from `/catalog/freshness and each metric's freshness_state` to
  `/catalog/health and each metric's freshness_status` fails as
  `Freshness names /catalog/health: expected false to be true`.
  `apps/web/lib/dataQuality.ts` was restored byte-for-byte afterwards.
- `npx tsc --noEmit` (apps/web) and `npm --prefix apps/web run lint` — clean.
- `pytest tests/unit/shared` — 205 passed, the register guards included.
- `python -m tests.support.catalog_evidence` renders WEB-065 `FULL`; the
  register is 375 rows.

## Remaining work

- None. Review is the remaining step.
