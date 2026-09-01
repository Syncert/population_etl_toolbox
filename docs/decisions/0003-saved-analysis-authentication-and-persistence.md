# ADR-0003: Saved-analysis authentication and persistence

- **Status:** Accepted
- **Date:** 2026-09-01
- **Accepted:** 2026-09-01 (human review; API-007's explicit precondition)
- **Decision owners:** API platform maintainers
- **Related work:** API-007 in the [API development plan](../plans/completed/API_DEVELOPMENT_PLAN.md)

## Context

API-007 delivers the saved-analysis-configuration API: versioned
query/filter/visualization intent persisted as API-owned data. The plan
forbids starting until an authentication, authorization, ownership, privacy,
retention, and deletion contract is approved. Everything before this point is
anonymous public reads over a read-only warehouse role; this is the first
write path and the first user identity the platform has.

Constraints already in force:

- The warehouse role (`api_reader`) is read-only and must stay that way;
  API-owned persistence uses separately owned tables and privileges
  (plan, "Stable warehouse boundary").
- Private or user-specific responses are never stored in the shared public
  cache (plan, "Reliability, security, and operations"). The API-006 cache
  covers only the public analytical GET prefixes, and request telemetry logs
  no headers or parameter values, so tokens and private content have no
  existing path into caches or logs — this contract keeps it that way.
- Posts, forums, comments, and social features are out of scope (plan
  non-goal); whatever identity ships here must not overreach into a social
  account system it cannot yet justify.

## Decision

### Identity and authentication

**Operator-provisioned personal access tokens, presented as
`Authorization: Bearer <token>`.** A CLI/script (like
`scripts/provision_api_readonly.py` today) creates a user row and prints a
random 256-bit URL-safe token exactly once; the database stores only
`sha256(token)`. No self-service signup, no passwords, no OAuth in this
iteration: the consumer is the project's own web application and its
operators, and every deferred alternative (sessions, OIDC) can be added
behind the same `Authorization` boundary later without moving stored data.
Verification is a constant-time compare of the hash; an invalid or absent
token on a protected route is `401` with a `WWW-Authenticate: Bearer`
header and a sanitized body. Tokens never appear in logs, cache keys,
responses, or error text; revocation is deleting the token row.

### Authorization and ownership

Every configuration row carries `owner_user_id`. All reads and writes are
scoped by the authenticated owner in SQL, not filtered after the fact.
Another user's configuration id answers `404` — indistinguishable from
"never existed", so ids cannot be enumerated across users. There are no
shared or public configurations in this iteration; sharing is future social
scope.

### Persistence boundary

A new `app_api` schema owned by a new `api_app_writer` role, provisioned by
a reviewed bootstrap script alongside the existing read-only grants:

- `app_api.user_account` — id, display label, token hash, created/revoked
  timestamps.
- `app_api.saved_analysis_configuration` — id, `owner_user_id`, name,
  monotonically increasing `version`, the configuration document (JSONB),
  created/updated timestamps.

`api_app_writer` has privileges on `app_api` only; `api_reader` keeps its
warehouse read-only grants and gets none here. The API uses a second,
separately configured engine for `app_api`, so warehouse reads and
application writes can never share a transaction or a privilege by
accident.

### Validation and concurrency

A configuration document is validated at write time against the same
capability and compatibility contracts live queries use (declared source
filters, the comparison policy), so a stored configuration cannot encode a
request the API would refuse; a document naming retired capabilities fails
with an actionable 422 on write and is reported, not silently repaired, on
read. Updates require the caller's expected `version`; a mismatch is `409`
with the current version, never a silent overwrite.

### Privacy, caching, telemetry

Configuration routes live outside the cacheable public prefixes and are
additionally marked `Cache-Control: private, no-store`. Request telemetry
keeps its route-shaped-facts-only contract (no headers, no bodies); token
values and configuration content never reach logs.

### Retention, export, and deletion

Configurations are kept until their owner deletes them — deletion is a hard
`DELETE`, effective immediately, and answered idempotently. `GET` of a
configuration is its own export (the document is the user's content,
returned verbatim). Revoking an account deletes its token; deleting an
account deletes its configurations in the same transaction. No analytics or
derived retention of user content.

## Rejected alternatives

- **Password accounts / OAuth now** — heavier identity surface than the
  single first-party consumer justifies; deferred until social scope forces
  real account management.
- **Warehouse-adjacent storage** (tables in `gold`/`gold_glossary`) —
  violates the read-only warehouse boundary outright.
- **Signed stateless tokens (JWT)** — revocation would need a denylist
  table anyway; opaque hashed tokens are simpler and strictly easier to
  revoke.

## Consequences

API-007 implements CRUD + list with ownership, optimistic concurrency,
validation-at-write, denial-path tests (cross-user access, enumeration,
revoked tokens), cache/telemetry isolation proofs, and the provisioning
script for `app_api`/`api_app_writer`. The deployment gains one schema, one
role, and one provisioning step; nothing about the public analytical surface
changes.
