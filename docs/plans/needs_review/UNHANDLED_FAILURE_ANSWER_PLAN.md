---
id: unhandled-failure-answer
branch: claude/iterate-plans-improvements-ir885c
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/api -q
  - python -m pytest tests/unit/shared -q
---

# An unhandled failure is answered like every other failure

## Plan status

- **Status:** Complete, awaiting review. Authored, claimed, and delivered
  2026-09-13 as catalog row API-088.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/api/telemetry.py`, `apps/api/middleware.py`

## Context

`telemetry.py` states the contract: every request gets a correlation id,
"echoed on the response and attached to one structured completion line:
method, route path, status, duration, and the cache disposition."

It does not hold for the one response a caller most needs it on. Starlette
builds the stack with `ServerErrorMiddleware` outermost — outside every
middleware the application adds — so when an endpoint raises, the 500 that
answers the caller never passes through this middleware's `send`. Probed
directly against the real module:

```
status: 500
x-request-id header: None
log: api_request method=GET path=/boom status=0 duration_ms=1.2 cache=- request_id=b171…
```

Three things are wrong in those two lines.

The response carries **no correlation id**. The id exists, and is in the log,
and the one person who needs it — someone opening a ticket about a 500 — is
the only one who cannot see it.

The completion line says **`status=0`**. Zero is not a status. The single
operational signal this module exists to produce reports the request that
failed hardest as having no outcome at all, so an error-rate query built on
`status>=500` misses exactly the errors it is for.

And the body is `text/plain` — Starlette's default `Internal Server Error`
string — where every other failure this API answers is a JSON `detail`
object. A client that parses the error body gets a parse error on top of the
500.

The existing tests for API-057 exercise a route that cannot raise, so all
three sit in a corner the row already claims but never reaches.

## Acceptance criteria

1. A request whose handler raises answers with the correlation id on the
   response, the same one the completion line carries.
2. Its completion line reports `status=500`, not `status=0`.
3. Its body is the same sanitized JSON shape as every other failure this API
   answers, and carries nothing derived from the exception.
4. The exception is not swallowed from the operator's view: its traceback is
   logged server-side, with the correlation id, so a log search from the
   caller's id reaches the stack.
5. Responses that do not raise are unchanged — same headers, same body, same
   completion line.
6. The behaviour is a `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing how handled failures answer. `HTTPException`, the sanitized 503s,
  and the 422s all already do the right thing and travel through this
  middleware normally.
- Reporting anything about the exception to the caller. The sanitized-failure
  discipline is the point, and this keeps it.

## What was built

`RequestTelemetryMiddleware` is now the correlation boundary for a failure
nothing else catches. It wraps the downstream call, and on an exception it
logs the traceback under the caller's own correlation id, then answers a
sanitized JSON 500 through its own `send` — so the response carries the
`X-Request-ID` the log line carries, and `status` is 500 in both places.

`INTERNAL_FAILURE_DETAIL` says nothing about the exception, for the same
reason `SERVICE_UNAVAILABLE_DETAIL` says nothing about the database: an error
body is not a place to publish deployment state.

It does not re-raise. Re-raising would hand the exception back to
`ServerErrorMiddleware`, which would try to answer a request already
answered; the traceback is not lost, because this logs it whole itself.

One case still re-raises: an exception after the response has already
started. There is then no status to correct and nothing safe to append, and
the failure is in the log above under the id the caller holds.

### The security headers, declared once

The synthesized response is created *outside* `SecurityHeadersMiddleware`,
which sits inside this one, so it is the single response that middleware
cannot reach — and before this it carried none of the declared headers
(nor did Starlette's default 500, for the same reason). The header set moved
out of the middleware body into `SECURITY_HEADERS`, read by both. Restating
them in `telemetry.py` would have been a second list to keep in step.

### A note on the test harness

`Starlette(...)` builds its own `ServerErrorMiddleware` *inside* itself, so
wrapping one in this middleware puts the middleware outside that boundary —
the reverse of how `create_app` mounts it, where `add_middleware` leaves
telemetry inside Starlette's error boundary and therefore the first thing an
escaping exception meets. The raising harness is a bare ASGI app so it models
the arrangement that ships.

## Validation

Run 2026-09-13 on this branch.

| Tier | Command | Result |
|---|---|---|
| Hardening unit | `python -m pytest tests/unit/api/test_operational_hardening.py -q` | 34 passed |
| API unit | `python -m pytest tests/unit/api -q` | 351 passed |
| Whole unit tier | `python -m pytest tests/unit -q` | 1418 passed |
| Register | `python -m tests.support.catalog_evidence` | 325 rows; API-088 is `FULL` |
| Lint | `ruff check apps/api tests/unit/api` | clean |
| Format | `ruff format --check` on the changed files | already formatted |

The defect was measured before the fix by probing the real module rather than
reasoning about it, and the fix was confirmed the same way — through
`create_app()` itself, with a route added that raises, not only through the
unit harness:

| | Before | After |
|---|---|---|
| status | 500 | 500 |
| `content-type` | `text/plain; charset=utf-8` | `application/json` |
| `X-Request-ID` | *absent* | `trace-prod` (the caller's own) |
| body | `Internal Server Error` | `{"detail": "The API failed to complete this request."}` |
| leaks the exception's text | no | no |
| completion line | `status=0` | `status=500` |

Not run, and why: the Docker-gated tiers (`make test-integration`,
`make test-e2e`, `make test-compose-smoke`) need a container runtime this
environment does not provide. No route, schema, or response contract changed,
so the reviewed OpenAPI snapshot is untouched.

## Acceptance criteria, as delivered

1. **Met.** `test_an_unhandled_failure_carries_its_correlation_id`, and
   confirmed through `create_app()`.
2. **Met.** Same test asserts `status=500` and the absence of `status=0`.
3. **Met.** `test_an_unhandled_failure_answers_the_sanitized_shape`, which
   also asserts the exception's own text does not reach the caller.
4. **Met.** `test_an_unhandled_failure_still_reaches_the_operator` asserts
   the record carries `exc_info` and the correlation id.
5. **Met.** `test_a_request_that_does_not_raise_is_unchanged` pins the
   success path's body, headers, and completion line.
6. **Met.** `API-088` in `docs/reference/TESTING_CONTRACT.md`, owned by the
   `api-unit` CI job, with `AUDITED_COUNTS["API"]` raised to 88.

Beyond the criteria: the failure response now also carries the declared
security headers, which neither it nor Starlette's default ever did.

## Remaining work

- None. The completion line still logs the raw request path, including path
  parameter values — a private packet or configuration id among them — which
  contradicts this module's own rule that parameter values never belong in
  logs. That is its own defect and its own plan.
