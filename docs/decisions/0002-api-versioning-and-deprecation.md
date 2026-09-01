# ADR-0002: API versioning and deprecation policy

- **Status:** Accepted
- **Date:** 2026-08-31
- **Accepted:** 2026-08-31
- **Decision owners:** API platform maintainers
- **Related work:** API-002 in the [API development plan](../plans/in_progress/API_DEVELOPMENT_PLAN.md)

## Context

The API grew as a vertical slice alongside the warehouse. Every route sat under an
unversioned `/api` prefix, so there was no way to change a response shape without
either breaking whatever was calling it or freezing the contract permanently. The
API platform work needs to do both things the current surface forbids: introduce
provider-neutral resources whose shapes differ from the MVP's, and retire
transitional behaviour that no consumer should have depended on.

The known consumers are twelve routes called by `apps/web`. There is no public
client, no partner integration, and no published contract. That makes this the
cheapest moment in the project's life to introduce a version boundary, and the
last moment before the API platform work starts producing shapes worth promising.

## Decision

**Versions are path segments.** A resource is served at `/api/v1/<resource>`.
Content negotiation through a custom media type or an `Api-Version` header was
rejected: a path is visible in a log line, a browser address bar, a curl command,
and a cache key without any special tooling, and the response cache already keys
on the path, so version separation comes for free rather than needing a `Vary`
that a proxy might drop.

**Every resource is served under both `/api/v1` and its original `/api` path.**
Routers declare version-relative prefixes and the application factory mounts each
router once per prefix, so the two surfaces are the same router, the same service
call, and the same response model. They cannot drift, because there is only one
definition; API-032 proves the parity holds operation by operation.

**Legacy responses announce their own retirement.** An unversioned `/api`
response carries `Deprecation: true`, a `Sunset` date, and a
`Link: </api/v1/...>; rel="successor-version"`, per RFC 8594. A consumer learns it
is on a retiring path from a response it was already reading, without anyone
having to find and notify it. The headers are added outside the response cache,
because they describe the route rather than the payload and must not depend on
whether Redis answered.

**The compatibility window is bounded and published.** The sunset date is a
constant in `apps/api/versioning.py`, not a vague intention. API-008 removes the
aliases, and only after evidence shows no required consumer still calls them; if
that evidence is not there when the date arrives, the date moves in a reviewed
change rather than the removal happening on schedule regardless.

**`/health` without a prefix is outside the policy.** It is the container and
load-balancer probe named in the deployment files. It carries no data contract,
it is not retiring, and a `Deprecation` header on it would be a false signal to
infrastructure. `/api/health` and `/api/v1/health` remain ordinary versioned
resources.

**A new version is additive.** Introducing `v2` means appending to
`SUPPORTED_VERSIONS` and moving `CURRENT_VERSION`. `v1` is never edited in place
once a consumer depends on it: a change that would break a `v1` client belongs in
`v2`, and a change that would not — a new optional response field, a new
endpoint, a widened accepted range — belongs in `v1`.

## What counts as breaking

| Change | Breaking |
| --- | --- |
| Removing or renaming an operation, field, or query parameter | Yes |
| Making an optional request parameter required, or narrowing its accepted range | Yes |
| Changing a field's type, or making a guaranteed field nullable | Yes |
| Changing the meaning of a value while keeping its name and type | Yes |
| Adding an optional query parameter, or a new operation | No |
| Adding a response field that existing clients can ignore | No |
| Widening an accepted range, or relaxing a bound | No |
| Changing ordering that was never documented as stable | Yes, in practice — so ordering is documented and tested instead |

The last row is the one that bites quietly. Deterministic ordering is part of the
contract precisely because a client that pages through results depends on it
whether or not anyone promised it.

## Enforcement

`tests/fixtures/api/openapi_contract.json` is a reviewed digest of every served
operation, parameter bound, response schema, and required-field set. API-031
fails when the served contract and the digest disagree, so a breaking change
cannot merge without the diff appearing in review. The snapshot is regenerated
deliberately with `python -m tests.support.regenerate_openapi_contract`;
regenerating it to make a red test green defeats the only mechanism that makes
this policy real.

## Consequences

The public surface roughly doubles in route count until API-008 retires the
aliases, which makes the generated documentation longer and gives the response
cache two key spaces for the same data. Both are accepted: the duplication is
mechanical, bounded by a published date, and cheaper than either breaking the web
application or freezing the MVP's shapes permanently.

Because the aliases share their routers with the versioned surface, any change to
a `v1` resource lands on the legacy path too. That is intentional for API-002,
where the surfaces are identical by construction. When API-003 through API-007
begin changing `v1` shapes, a resource whose legacy shape must be held still gets
its own frozen legacy router at that point, and API-032's parity assertion is
narrowed to the pairs that are still meant to match.
