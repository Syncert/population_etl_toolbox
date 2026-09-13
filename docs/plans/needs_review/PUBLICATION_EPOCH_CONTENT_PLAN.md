---
id: publication-epoch-content
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# The cache epoch rotates when the published state changes

## Plan status

- **Status:** In progress. Authored and claimed 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/freshness.py`
- **Next pickup:** read the whole published state, not its maximum.

## Context

The response cache keys every body on a publication epoch, and `freshness.py`
states the guarantee plainly:

> a republication rotates the key and a stale body cannot be served for the
> whole TTL

The epoch is `MAX(last_publication_time)` over
`gold_glossary.publisher_harvest_state`. That does not deliver the
guarantee, and the warehouse already knows why. Migration 016 fixed the same
mistake one layer down, in the harvest guard:

> The glossary harvest skipped whenever a publisher's `publication_time` had
> not advanced. That time is derived from the *facts* every publisher reads,
> so a change to what the publisher SAYS -- a metric's identity, display
> name, units, grains, lineage, or the set of keys it emits -- moved nothing
> the guard could see.

Its answer was `last_content_fingerprint`, a digest of the content the
harvest writes. The harvest now runs when *either* input moves. The cache
epoch still reads only the first one, so the exact case migration 016 exists
for — content changed, publication time did not — republishes the catalog
and rotates nothing. `/catalog/metrics`, `/catalog/capabilities`, and every
cached observation body keep serving the identities the warehouse no longer
publishes, for the whole TTL, and the epoch reports that as fresh.

`MAX` has a second problem of its own. `last_publication_time` is the
*publisher's* declared time, carried through from the ready event, not a
harvest clock the warehouse controls. Taking a maximum across seven
independent publishers assumes one clock between them. A source republishing
with a time behind another source's — a backfilled release, a correction
harvested late, a publisher whose lifecycle timestamps lag — leaves the
maximum where it was, and the same silence follows.

An epoch is a cache key, not a date. It needs one property: it changes when
the published state changes. A digest over the whole state has that
property; a maximum over one column of it does not.

## Acceptance criteria

1. The epoch changes when any source's recorded publication state changes —
   its publication time, its content fingerprint, or its source watermark —
   and not only when the newest publication time advances.
2. A state that has not changed yields the same epoch, so an unchanged
   warehouse keeps its cache.
3. The epoch stays a short, opaque, URL-safe token: it is a cache-key
   component, and nothing may read a date out of it.
4. An empty harvest-state table still answers, and a failed read still keeps
   the last known epoch (both unchanged from today).
5. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the cache TTL, the freshness window, or the key's other
  components.
- Making the epoch per-source. A single epoch over-invalidates, which costs
  cache hits; it never serves a stale body, which is the property that
  matters.

## Validation

To be recorded by the agent that claims this.

## Remaining work

- Everything.
