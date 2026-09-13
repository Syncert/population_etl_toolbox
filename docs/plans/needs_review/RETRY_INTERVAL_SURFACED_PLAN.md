---
id: retry-interval-surfaced
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
---

# A rate-limited reader is told how long to wait

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of how the web app renders API failures.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/api/client.ts`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

The API's rate limiter answers a limited request with a stable body and a
`Retry-After` header:

```python
RATE_LIMITED_DETAIL = "rate limit exceeded; retry after the indicated interval"
```

The web client captures that header — `ApiError.retryAfter` is parsed, typed,
and unit-tested — and then never uses it. `apiErrorMessage` renders
`status ${status}${detail ? ": " + detail : ""}`, so what a reader sees is:

> status 429: rate limit exceeded; retry after the indicated interval

The sentence points at an interval and withholds it. The one number that
tells the reader what to do is in hand, on the error object, and is dropped
on the way to the screen.

This matters more after API-075 than before. The limiter used to key every
request on the proxy in front of the API, so the budgets were one budget for
the whole deployment and an individual reader rarely met them; now that the
client is metered as itself, a reader who paginates a large catalog or drives
the explorer hard is the one who meets the limit, and the interval is the
answer to "what now".

## Objective

When the API publishes how long to wait, the message says how long.

## Acceptance criteria

1. An `ApiError` carrying a positive `retryAfter` renders the interval in
   seconds alongside the status and the API's own detail.
2. An error without one is unchanged, exactly as it renders today —
   including a `retryAfter` that is zero, absent, or not a finite number.
3. Nothing else about the message changes: it stays status-first, and the
   API's `detail` still travels verbatim.
4. The behavior is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Retrying automatically. A client that retries on its own turns one reader's
  limit into a louder version of the same request; the reader decides.
- Surfacing `X-Request-ID`. It is the other thing the API publishes and the
  client drops, but where a 32-character correlation id belongs on screen is
  a design question this plan does not answer.
- Changing the API's `detail` wording.

## Evidence

### The gap, established first

`a rate-limited message says how long to wait` failed first: a `429` carrying
`retryAfter: 12` rendered as
`status 429: rate limit exceeded; retry after the indicated interval` — the
interval absent from a sentence that names it.

The companion test, that an error carrying no interval is unchanged, passed
before and after, which is the point of having it: it pins the four shapes
that must not acquire a parenthetical — `null`, `0`, `undefined`, and `NaN`.

### What changed

Five lines in `apiErrorMessage`. The interval is appended only when it parses
as a finite number above zero; everything else about the message is as it
was, status-first with the API's `detail` verbatim.

Nothing retries on its own. A client that retried automatically would turn
one reader's limit into a louder version of the same request, and the whole
point of the header is that the reader can decide.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:unit` | 236 passed |
| `npm --prefix apps/web run test:browser` | 60 passed (Chromium) |
| `npm --prefix apps/web run lint` / `typecheck` | clean |
| `pytest tests/unit -q` | 1380 passed |
| `python -m tests.support.catalog_evidence` | 305-row register renders; WEB-040 is `FULL` |

### Not run

No browser or smoke tier is implicated: this is one pure function over an
error object, and the unit tier drives every shape of it. Reproducing a real
`429` would need the deployment's rate limits enabled against a live API,
which is what API-056 already covers on the server side.

## Remaining work

None.
