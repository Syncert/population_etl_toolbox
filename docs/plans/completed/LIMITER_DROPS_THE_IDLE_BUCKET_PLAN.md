---
id: limiter-drops-the-idle-bucket
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - pytest tests/unit/api/test_operational_hardening.py
---

# The limiter drops the bucket that carries nothing, not the busiest one

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/ratelimit.py`

## Context

The limiter bounds how many clients it tracks, and says what it drops:

> Bound on tracked clients; beyond it **the oldest state is dropped**, which
> can only under-throttle briefly and keeps memory bounded under address
> churn.

The code drops the oldest *inserted* entry:

```python
if len(self._buckets) >= _MAX_TRACKED_BUCKETS:
    self._buckets.pop(next(iter(self._buckets)))
```

A `dict` iterates in insertion order and nothing re-inserts a bucket on use,
so `next(iter(...))` is the client that arrived first — which, among clients
still sending traffic, is the one that has been sending it longest. And an
evicted bucket is recreated at full capacity, so eviction hands out a fresh
whole budget.

Reproduced directly, with the bound lowered to three:

```
heavy exhausted -> wait 1.0
tracked: [('analysis', 'heavy'), ('analysis', 'b'), ('analysis', 'c')]
after churn: [('analysis', 'b'), ('analysis', 'c'), ('analysis', 'churn-1')]
heavy now waits: 0.0
```

`heavy` had spent its entire minute and was being refused. One new address
arrived, and `heavy` — the only exhausted client in the table — is the entry
dropped, while two near-full buckets holding no useful state are kept. Its
next request is granted.

So under address churn the limiter stops limiting the sustained client it
exists to limit, and the comment's "can only under-throttle briefly" is true
of the table as a whole and false of that client: every new address resets
it again.

The right rule follows from what a bucket is. **An absent bucket and a full
bucket are the same thing** — both grant a whole budget — so the entry worth
dropping is the one closest to full, and the entry closest to full is the one
used least recently: a bucket refills to capacity after at most sixty seconds
of inactivity at any configured rate, because capacity is the per-minute rate
and it refills at rate/60 per second. Least-recently-used is therefore not a
heuristic here; it is the policy that can only ever drop state that had
already expired.

## Acceptance criteria

1. When the table is full, the bucket dropped is the least recently used, not
   the first inserted. A client still sending traffic keeps its bucket.
2. An exhausted client cannot regain its budget by waiting for other
   addresses to arrive.
3. The eviction stays O(1) per insertion — no scan of the table.
4. Every existing API-056 and API-101 behaviour is unchanged: the cost-class
   split, the continuous refill, the trusted-proxy identity, the exemptions.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (API-104).

## Non-goals

- Raising `_MAX_TRACKED_BUCKETS`. The bound is what keeps memory bounded
  under churn; the bug is which entry it sacrifices.
- Sharing limiter state between processes. The module already records that
  state is in-process and that a multi-process deployment multiplies the
  budget; that is a deployment fact, not this defect.

## Validation

**Failing first**, on the two nodes that state the behaviour:

```
FAILED test_the_bucket_dropped_is_the_idle_one_not_the_busiest
E   AssertionError: the exhausted client regained its budget because another
E   address arrived: under churn the limiter stops limiting the one client it
E   exists to limit
FAILED test_a_client_still_sending_traffic_keeps_its_bucket
E   AssertionError: assert ('analysis', 'steady') in {('analysis',
E   'arriving'): …, ('analysis', 'one-shot'): …}
```

Confirmed by stashing the implementation and re-running: both fail against
the old eviction and pass against the new one.

**The first version of the first test was wrong and the fix caught it.** It
had the exhausted client spend its minute, then two addresses arrive, then a
third — and asserted the exhausted client was still refused. It failed
against the *fixed* code, correctly: the exhausted client had made no request
after the other two arrived, so it genuinely *was* the least recently used
entry, and dropping it is the policy working. The scenario did not express
"still sending traffic" at all. Corrected so the exhausted client sends again
between the quiet arrivals and the new one, which is the claim: use, not
arrival, decides what the bound sacrifices. It still fails against the old
code, because the old code never re-inserts a bucket, so the first-arrived
client is the head no matter how recently it was served.

**The fix.** `_take_token` pops its bucket and re-inserts it whether it is
new or not, so the head of the insertion-ordered mapping is the least
recently used entry. `next(iter(...))` then drops that one, in one step. The
third node,
`test_eviction_does_not_scan_the_table`, pins the cost: it fills the table to
the real bound and asserts the victim is found at the head rather than by
comparing ten thousand timestamps. It passes before and after — it is a guard
on the property that makes this policy usable, not evidence of the fix.

**Why LRU is not a heuristic here.** Capacity is the per-minute rate and
refill is rate/60 per second, so a bucket untouched for sixty seconds has
refilled to capacity, and a full bucket grants exactly what an absent one
does. Least-recently-used is therefore the policy that can only ever drop
state which had already expired.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Unit | `pytest tests/unit` | **1470 passed** (was 1467) |
| Unit, this file | `pytest tests/unit/api/test_operational_hardening.py` | 44 passed (was 41) |
| Integration | `pytest tests/integration -m "integration and (redis or database) and not slow"` | 140 passed, 2 skipped |
| Lint / format | `ruff format --check .`, `ruff check .` | clean, 442 files |

**Register.** 355 rows.

## Remaining work

- None.
