---
id: rate-limit-client-identity
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - pytest -m "unit and api" tests/unit/api
  - ruff check .
---

# The rate limiter knows which client it is limiting

## Plan status

- **Status:** Accepted 2026-09-14 (Ready for review. Authored, claimed, and delivered 2026-09-13 from an investigation of the API's operational middleware.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/api/ratelimit.py`, `apps/api/main.py`,
  `src/data_ingestion_toolbox/config.py`, `infra/docker/`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

`apps/api/ratelimit.py` states its contract in its first line: "Per-client
rate limiting with declared cost classes". It identifies that client as

```python
def _client_of(self, scope):
    client = scope.get("client")
    return client[0] if client else "unknown"
```

— the address of the immediate TCP peer. In every topology this repository
actually deploys, that peer is a reverse proxy, not a client:

- `infra/docker/docker-compose.yml` publishes the API on `127.0.0.1` only and
  serves the public surface through the `web` service, whose
  `next.config.mjs` rewrites `/api/:path*` to `http://api:8000`. Every public
  request reaches the API from the `web` container's address.
- `infra/docker/docker-compose.test.yml` and the smoke tier put
  `infra/web/nginx.conf` in front for the same reason.

So the two buckets — 600 catalog and 240 analysis requests per minute in the
shipped compose defaults — are not per client. They are one budget for the
entire deployment, keyed by the proxy. One client's loop exhausts every other
client's budget, and the limiter that exists to protect the warehouse becomes
a way to deny service to everyone at 601 requests a minute.

Both proxies already send the address the API needs. `infra/web/nginx.conf`
sets `X-Forwarded-For` and `X-Real-IP` on `/api/`, and Next's rewrite proxy
passes an inbound `x-forwarded-for` through to the destination unchanged
(verified in this environment against the built standalone server: a request
carrying `X-Forwarded-For: 203.0.113.7` arrived at the upstream with that
header intact, and a request without one arrived with none — Next forwards
the header but never invents it). The API discards it.

Reading that header unconditionally would be worse than ignoring it: any
client could then mint a fresh budget per request by varying a header it
controls. The header is evidence only when the hop that set it is one the
deployment trusts, which is a configuration fact the API cannot infer.

## Objective

The rate limiter identifies a client by the address a *trusted* proxy chain
reports, falls back to the peer address, and can never be given a new budget
by a header from an untrusted peer.

## Acceptance criteria

1. A new setting, `API_TRUSTED_PROXY_IPS`, accepts a comma-separated list of
   addresses or CIDR blocks. Empty — the default — preserves today's
   behavior exactly: the peer address, headers ignored.
2. When the peer is trusted, the client is the right-most `X-Forwarded-For`
   entry that is not itself a trusted proxy, so a chain of declared hops
   resolves to the address that entered it.
3. When the peer is *not* trusted, `X-Forwarded-For` is ignored entirely. A
   spoofed header from a direct client shares the peer's bucket and buys no
   budget.
4. Degenerate input degrades to the peer address rather than inventing an
   identity: an absent header, an empty header, a chain of only trusted hops,
   and an unparseable entry.
5. A malformed `API_TRUSTED_PROXY_IPS` fails at startup with a message naming
   the entry, rather than silently trusting nothing.
6. The consumer guide states how a client is identified for rate limiting;
   the compose deployment declares the setting; and the behavior is a
   `TESTING_CONTRACT.md` catalog row with CI ownership.

## Non-goals

- Changing the limits, the cost classes, the bucket algorithm, or the
  in-process state decision — all of which are recorded and out of scope.
- Making the telemetry middleware or the access log report the same address.
- Trusting `X-Real-IP`: a single header cannot express a chain, and the
  deployment's own proxy sets both.
- Rate-limiting by API token for the authenticated resources.

## Evidence

### The gap, established first

Seven tests in `tests/unit/api/test_operational_hardening.py` were written
against the contract above and failed 10/10 (the degenerate-input case is
parameterised six ways) before any implementation existed: the middleware
took no `trusted_proxies` argument, so every one raised `TypeError`.

The proxy-forwarding claim was not assumed. `infra/web/nginx.conf` sets
`X-Forwarded-For` on `/api/` in the checked-in configuration. Next's rewrite
was measured rather than read: the web app was built with
`API_ORIGIN` pointed at a header-echoing server, the standalone server run,
and two requests sent through `/api/v1/health`. One carrying
`X-Forwarded-For: 203.0.113.7` arrived upstream with that header intact; one
sent without it arrived with none. Next forwards the header it receives and
never invents one — so the address survives to the API whenever something in
front of the web app sets it, and the API was throwing it away.

### What changed

- `RateLimitMiddleware` takes `trusted_proxies`, parsed once at construction
  into `ipaddress` networks; a bad entry raises at startup naming the entry.
- `_client_of` returns the peer unless the peer is declared, and otherwise
  the right-most `X-Forwarded-For` entry that is not itself declared. A hop
  that wrote something unparseable falls back to the peer rather than keying
  a bucket on attacker-chosen text.
- `X-Real-IP` is deliberately not read: one address cannot express a chain,
  so a two-hop deployment would resolve to the inner hop and collapse every
  client into one bucket again — the defect this plan closes.
- `Settings.api_trusted_proxy_ips` reads `API_TRUSTED_PROXY_IPS`, and
  `create_app` passes it to the middleware. A test asserts the value reaches
  the built application, because a setting that is accepted and never used is
  the same single bucket with more code behind it.
- Both compose files declare the variable (empty default: no behavior change
  without a deployment decision), and both `stack.env.example` files set the
  private ranges Docker allocates compose networks from, with the reason.

### Commands

| Command | Result |
| --- | --- |
| `pytest tests/unit/api/test_operational_hardening.py -q` | 25 passed |
| `pytest -m "unit and api" tests/unit/api -q` | 299 passed |
| `pytest tests/unit -q` | 1366 passed |
| `python -m tests.support.catalog_evidence` | 296-row register renders; API-075 is `FULL` with seven named nodes |
| `ruff check .` | All checks passed |
| `ruff format --check .` | 439 files already formatted |

The reviewed OpenAPI snapshot is unchanged, as it should be: this is
middleware behavior, not a route contract.

### Not run

`make test-compose-smoke`, `make test-integration`, and `make test-e2e` need
Docker, which this environment has no daemon for. The conclusion they would
carry — that the shipped stack starts with the new variable present — was
verified instead by keeping the compose default empty, which is the value the
middleware already had before this change, and by the unit test that proves
the empty default reproduces the previous behavior exactly.

## Remaining work

None.
