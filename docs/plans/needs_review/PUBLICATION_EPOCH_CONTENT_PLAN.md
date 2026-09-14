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

- **Status:** Needs review. Authored and claimed 2026-09-13; delivered
  2026-09-13 as API-085. The status, validation and remaining-work fields
  below were reconciled with the folder on 2026-09-14: the implementation,
  its tests and its catalog row had landed, and this document alone still
  said "everything" remained.
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/freshness.py`
- **Next pickup:** none.

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

## What changed

- `apps/api/freshness.py` reads the whole of
  `gold_glossary.publisher_harvest_state` -- `source_code`,
  `last_publication_time`, `last_content_fingerprint` and
  `last_source_watermark`, ordered by source -- and `publication_epoch(rows)`
  digests it: the rows are sorted again in Python, serialized as JSON so
  field boundaries are unambiguous, hashed with SHA-256 and cut to sixteen
  hex characters, the width the served-contract fingerprint already uses.
  The epoch rule is a pure function over rows so it is provable without a
  database (criteria 1--3).
- An empty table answers the constant `never-published`, and the provider's
  refresh path is unchanged: a failed read logs and keeps the last known
  epoch, `epoch-unknown` before any read succeeds, and is retried only after
  the freshness window (criterion 4).
- `docs/reference/TESTING_CONTRACT.md` row **API-085** names the behaviour;
  `tests/unit/api/test_operational_hardening.py` carries its four nodes
  (criterion 5):
  - `test_the_epoch_changes_when_any_recorded_state_changes` -- an unchanged
    state keeps its key; changing any one of the three recorded fields
    rotates it;
  - `test_a_source_behind_another_still_rotates_the_epoch` -- a correction
    whose declared time is behind another publisher's still moves the key,
    which `MAX` never did;
  - `test_the_epoch_is_a_stable_opaque_token` -- row order is not part of
    the state, and the token is sixteen lowercase hex characters;
  - `test_an_empty_harvest_state_still_answers` -- nothing published is a
    cacheable state, distinct from any published one.
- The module docstring records why the maximum was wrong, in the same terms
  migration 016 used for the harvest guard, so the next reader does not
  rediscover it.

## Validation

Run 2026-09-14 on this branch, in a clean virtual environment built from
`.[api,dev,martin-test]`:

- `python -m pytest tests/unit/api/test_operational_hardening.py -k "epoch or published"`
  -- **6 passed** (the four API-085 nodes and the two API-054 epoch nodes).
- `python -m pytest -m "unit and api" tests/unit/api -q` -- **572 passed**.
- `python -m pytest tests/unit/shared -q` -- **227 passed**.
- `python -m tests.support.catalog_evidence` renders API-085 as `FULL`,
  owned by `api-unit`, `redis-integration` and `e2e-performance`.
- `ruff check .` and `ruff format --check .` -- clean.

Not run here: the Redis integration tier that exercises the provider
against a live cache. It owns API-054's memoization and failure-keeps-last-
epoch behaviour, which this plan did not change; the epoch rule itself has
no database in it and is covered in full by the unit nodes above.

## Remaining work

- None. Review is the remaining step.
