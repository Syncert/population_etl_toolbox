---
id: one-snapshot-for-stored-work
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest tests/unit/api/test_saved_analysis.py tests/unit/api/test_evidence_packets.py
  - pytest tests/integration/api -m "integration and database"
---

# A saved-work listing counts the rows it returns

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/services/saved_analysis_service.py`,
  `apps/api/services/evidence_packet_service.py`

## Context

API-100 gave the warehouse engine `REPEATABLE READ` so that a page and its
total cannot describe different sets of rows, and its catalog row names the
scope it left behind:

> the application-storage engine, which writes, is unchanged

That exclusion was right about the engine and wrong about the reads. The two
listings served off the application engine have exactly the shape API-100's
own failure clause describes -- "every paged read in the API has the same
two-statement shape":

```python
total = int(storage.execute(_COUNT, {"owner_user_id": owner_user_id}).scalar() or 0)
rows = storage.execute(_SELECT_PAGE, {...}).mappings().all()
```

`app_api` runs at PostgreSQL's default `READ COMMITTED`, so each of those two
statements takes its own snapshot. A caller creating a configuration in one
tab while another lists them can be answered `total: 3` beside four items --
or `total: 4` beside a first page that does not contain the row they just
saved, with the fourth waiting on a second page the client is not told to
ask for.

Flipping this engine to `REPEATABLE READ` is the fix API-100 correctly
declined, and the reason deserves recording because it is what makes the
engine-level answer wrong rather than merely unnecessary: this engine carries
the optimistic-concurrency `UPDATE`, whose whole design is that a stale
`expected_version` matches no row and answers 409. Under `REPEATABLE READ` a
second writer racing the first does not see zero rows -- PostgreSQL raises
`could not serialize access due to concurrent update`, an `OperationalError`,
which this router turns into a sanitized 503. The correct 409 would become a
transient failure the caller cannot act on.

## Acceptance criteria

1. Each listing reports a total counted over the same rows its page was taken
   from, whatever commits during the request. Proved behaviourally against a
   real PostgreSQL, not by asserting a configuration string.
2. The fix is the one API-084 already established for this defect: the total
   and the page are computed in one statement, so there is nothing for a
   commit to land between at any isolation level.
3. The application engine's isolation is unchanged, and a racing update still
   answers 409 rather than a serialization failure.
4. Ordering, paging bounds, and the empty-page answer are unchanged: an
   `offset` past the end still reports the true total beside no items.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-103).

## Non-goals

- Raising the application engine's isolation level. See above: it would trade
  a correct 409 for a 503.
- Changing what either listing returns. The total was always meant to be the
  count of the caller's rows; this makes it that.

## Validation

**Failing first**, on both listings, with the concurrent commit landed at the
exact seam:

```
FAILED test_a_listing_counts_the_rows_it_returns[analysis-configurations-…]
FAILED test_a_listing_counts_the_rows_it_returns[evidence-packets-…]
E   AssertionError: evidence-packets answered total 3 beside 4 items: the
E   page and the total were taken from two different snapshots
E   assert 4 == 3
```

The seam is keyed on the statement, not on execution order, because
`require_account` reads on the same session first: the listener fires after
the statement carrying the `COUNT(*)`, which is where the old
implementation's second statement followed and where the fixed
implementation has already finished reading. That keying is what makes the
same test meaningful against both implementations rather than silently
passing against the fixed one, and the test asserts the commit landed
(`stored_work.landed == 1`) so it cannot pass by never reaching the seam.

**The fix.** Each listing became one statement — `owned`, then `counted` and
`page` over it, joined `counted LEFT JOIN page ON TRUE`. One statement is one
reading at any isolation level, which is API-084's answer to this same defect
and needs nothing from the engine. `counted` always yields exactly one row,
so an empty page is the count-only row and still reports the true total; the
services drop it by its null key.

**Two pre-existing unit fakes had to change**, and they are the second piece
of evidence. Both modelled the old two-statement shape — a `SELECT COUNT(*)`
branch answering a scalar, and a page branch answering rows with no total.
They were updated to model the real statement (every page row carries the
total; an empty page is the LEFT JOIN's count-only row) rather than by making
the services tolerate a missing key with `.get`, because a fake that does not
have the statement's shape is the thing that let the defect live in a module
with 64 passing unit nodes.

**The write path is unchanged, and asserted to be.**
`test_a_racing_update_still_answers_conflict_not_unavailable` pins the 409
with its current version. It passes before and after — it is not evidence of
a fix but a guard on the reason the engine-level answer was declined, so the
next reader who sees a two-snapshot read on this engine does not "finish"
API-100 by flipping `isolation_level` and trading a correct 409 for a 503.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | 1467 passed |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | **140 passed**, 2 skipped (was 135) |
| End-to-end | `pytest tests/e2e -m e2e`, fresh `e2e_fresh_test` | 9 passed (1:59) |
| Frontend units | `npm --prefix apps/web run test:unit` | 295 passed |
| Lint / format | `ruff format --check .`, `ruff check .` | clean, 442 files |

The five new integration nodes are the two listings times the seam test and
the empty-page test, plus the conflict guard.

**Register.** 354 rows.

## Remaining work

- None.
