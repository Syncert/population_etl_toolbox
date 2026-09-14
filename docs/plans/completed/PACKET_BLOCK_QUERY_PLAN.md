---
id: packet-block-query
branch: claude/iterate-plans-improvements-ir885c
depends_on:
  - explorer-saved-reduction
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A packet block replays the request its own envelope records

## Plan status

- **Status:** Accepted 2026-09-14 (Complete, awaiting review. Authored, claimed, and delivered 2026-09-13 as catalog row WEB-048.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/evidencePackets.ts`,
  `apps/web/components/EvidencePacketBuilder.tsx`,
  `apps/web/components/SourceExplorerPage.tsx`,
  `apps/web/lib/savedAnalysis.ts`
- **Depends on:** WEB-047, which taught `explorerDocument` to record a
  reduction. This is the other producer of the same document.
- **Next pickup:** none.

## Context

`EvidencePacketBuilder.attachSavedView` turns a saved explorer view into an
analytical block: an envelope on one side, an `AnalysisDocument` on the other.
It builds that document as a hand-written literal:

```ts
{
  kind: "observations",
  metric_code: String(chart.metricCode || ""),
  scope: envelope.scope,
  release: envelope.release || null,
  filters: { geo_level: ..., geo_id: ... },
}
```

That is a second construction of a document `explorerDocument` already knows
how to build, and it is a weaker one in two ways.

**It records no reduction.** The explorer's map asks `/observations` for
`newest_per_geography=true`, and the envelope beside the block records that
request verbatim in `api_query`. The document does not, so both fields
default to false and the block replays the source's whole latest publication.
For Census PEP that is every estimated year of the current vintage. The block
therefore does not reproduce the request its own envelope records — in the
one resource whose entire purpose is that a reader can re-derive the evidence
without this application, and which its owner hands to somebody else.

WEB-047 fixed this for the saved-configuration path. This path was not
looked at, because it does not go through `explorerDocument` at all.

**It can build a contradiction.** `release` is copied unconditionally, so a
chart carrying a release identity without an as-released scope produces
`scope: "latest"` with a `release`, which `validate_document` refuses:

> `release can only be combined with scope=as_released`

`explorerDocument` drops exactly that pairing, and the four the reductions
add, before the document is ever built. A second construction has to
re-learn each rule, and today has learned none of them.

Underneath both is the saved chart itself. The explorer's browser-local save
records `apiQuery` but not what the query *asked*: no `scope`, no `release`,
no reduction. So even a correct document builder has nothing to read. That is
also why WEB-047 left `migrationCandidates` recording no reduction — the
chart did not carry one to record.

## Acceptance criteria

1. An analytical block's document is built by the same functions the explorer
   saves through (`explorerDocument`, `comparisonDocument`), so every rule
   about what a document may contain lives in one place.
2. A saved explorer view records what its request asked for — scope, release,
   and the reduction — so the envelope and the document describe the same
   request.
3. A block filled from a map view replays the map: its document carries the
   reduction its envelope's `api_query` shows.
4. A chart carrying a release without an as-released scope produces a
   document the API accepts, not one it refuses.
5. A chart saved before this change, carrying none of the new fields, still
   attaches and still produces exactly the document it does today.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the envelope's own fields or what the packet reports as missing.
- Changing what the API stores or validates; every rule this needs is
  already served.

## What was built

`documentFromSavedChart` in `evidencePackets.ts` builds an attached view's
query by delegating to `explorerDocument` or `comparisonDocument`, and
`attachSavedView` calls it instead of writing a literal. Every rule about
what a document may contain — which reduction it recorded, which pairings the
API refuses — is now in the one place that already knew them.

A two-measure view stays a plain `comparisonDocument` with no scope or
release. That is deliberate: the comparison route serves no scope, the saved
comparison chart records none, so the envelope's default `latest` and the
document's default `latest` agree and the API's envelope/query cross-check
has nothing to contradict. Adding scope to a document whose route ignores it
would store a parameter that means nothing.

The explorer's browser-local save now records `scope`, `release`, and
`newestPerGeography` beside the `apiQuery` it already recorded. That is what
made both halves possible: a consumer rebuilding the query from a chart had
only the filters, and `apiQuery` is a URL, not something to parse back into a
document.

With the chart carrying it, `planLocalMigration` carries it too — which
closes the limitation WEB-047 recorded, where a migrated map could not claim
a reduction because the chart it came from held none.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Web unit | `npm --prefix apps/web run test:unit` | 264 passed (21 files) |
| Web browser | `npm --prefix apps/web run test:browser` | 65 passed |
| Lint | `npm --prefix apps/web run lint` | clean |
| Types | `npm --prefix apps/web run typecheck` | clean |
| Build | `npm --prefix apps/web run build` | succeeded |
| Bundle budget | `npm --prefix apps/web run check:bundle` | every route within budget |
| Register | `python -m pytest tests/unit -q` | 1408 passed; WEB-048 is `FULL` |

Failing-first was confirmed on both tiers. The four unit tests failed with
`documentFromSavedChart is not a function`; more usefully, restoring the
hand-built literal with everything else in place fails the browser test on
`expect(filled.document.newest_per_geography).toBe(true)` —
`Expected: true, Received: undefined` — which is the defect exactly: the
envelope's `api_query` carries `newest_per_geography=true` and the stored
query says nothing about it.

The browser tier needs `PLAYWRIGHT_CHROMIUM_EXECUTABLE=/opt/pw-browsers/chromium`
in this environment.

Not run, and why: the Docker-gated tiers (`make test-compose-smoke`,
`make test-web-smoke`, `make test-e2e`) need a container runtime this
environment does not provide. Nothing here changes a served response or a
deployment surface.

## Acceptance criteria, as delivered

1. **Met.** `attachSavedView` holds no document literal.
2. **Met.** The explorer's saved chart records scope, release, and the
   reduction; `envelopeFromSavedChart` already read the first two.
3. **Met.** `test("a block replays the request its envelope records")` asserts
   the stored document's reduction against the envelope's own `api_query`.
4. **Met.** `test("a release without an as-released scope is dropped, not
   stored")` — the rule `explorerDocument` already enforces, now reached from
   this path too.
5. **Met.** Two tests cover the pre-change chart, one for attaching and one
   for migrating; the existing packet browser tests, which seed exactly such
   charts, pass unmodified.
6. **Met.** `WEB-048` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `frontend` CI job, with `AUDITED_COUNTS["WEB"]` raised to 48.

## Remaining work

- None.
