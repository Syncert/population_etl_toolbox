---
id: self-service-accounts
branch: claude/self-service-accounts
depends_on:
  - self-service-identity
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit -q
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint ; npm --prefix apps/web run typecheck
  - ruff format --check . ; ruff check .
---

# A visitor can hold an account of their own

## Plan status

- **Status:** Claimed and **in progress.** The API half is built and verified
  against a real PostGIS 16 database. The web sign-in surface (deliverable 4)
  is not started, and one acceptance criterion needs a credential no agent
  container holds. `self-service-identity` was recorded `approved` by Nick on
  2026-09-20, against
  [ADR-0005](../../decisions/0005-self-service-accounts.md), which has been
  `Accepted` since 2026-09-16.
- **Last updated:** 2026-09-20
- **Current milestone:** deliverable 4, the web sign-in surface.

### Checkpoint, 2026-09-20

Branch `claude/plans-iteration-2026-09-20`, four commits. Everything below is
inspectable in the repository; nothing here is a plan for work rather than a
record of it.

**Done, with evidence.**

| Deliverable | Where | Evidence |
| --- | --- | --- |
| 1. Account lifecycle in `app_api` | `sql/bootstrap/002_app_api.sql`, `scripts/provision_app_api.py` | API-149, 6 integration tests |
| 2. Credential issuance behind the existing boundary | `apps/api/auth.py`, `apps/api/oidc.py`, `apps/api/services/identity_service.py`, `apps/api/routers/identity.py` | API-150 (41 unit), API-151 (29 integration) |
| 3. Bounds on the first unauthenticated write | `apps/api/ratelimit.py`, `identity_service._resolve_account` | API-153 (3 unit) |
| 4. The web sign-in surface | -- | **not started** |
| 5. Migration | `002_app_api.sql`'s migration block | API-149, against a schema built in the *previous* shape |
| 6. Contract documentation | `API_CONSUMER_GUIDE.md`, `TESTING_CONTRACT.md` | the gates in `tests/unit` that enforce both |

Beyond the numbered deliverables, and required by the ADR rather than by the
scope list: account export, hard deletion with a freshness requirement, the
public display name, and the operator actions in §4 (`--revoke-sessions-label`,
`--block-account-label`, `--unblock-account-label`). API-152, 15 integration
tests.

**Acceptance criteria.**

- [x] Register, sign in, save, sign out, sign in again, find the work --
      `test_a_visitor_saves_work_signs_out_signs_in_again_and_finds_it`. Saved
      an analysis; an evidence packet is the same owner-scoped write through
      the same credential and the same `require_account`.
- [x] Every existing denial path holds under the new credential -- cross-user
      access answers 404, a refresh token and a transaction handle are refused
      as bearer tokens, revoked and expired credentials fail. The existing
      isolation tests pass unmodified; what changed in them is how a fixture
      *creates* an account, not what it asserts.
- [x] Anonymous public reads unchanged -- no route moved into or out of
      `CACHEABLE_ROUTERS`, and the reviewed OpenAPI snapshot's diff is
      additive: five sign-in operations, three account operations, four
      schemas, and a `403` on one route. The existing sweeps pass.
- [x] No credential, identifier, or account content in logs, cache keys,
      URLs, or error text -- one stable refusal string, one classification
      word in the log line, no credential value in the export, and every
      stored token a 64-character digest.
- [x] Registration and sign-in are bounded, and the bound is tested --
      API-153, plus the deployment-wide account-creation ceiling.
- [x] Deleting an account does what the ADR says, in one transaction, with
      evidence -- API-152, including that a stale session destroys nothing on
      its way to a 403.
- [ ] The browser holds one credential in one place -- **deliverable 4.**

**Not run here, and why.**

- The browser tier (`npm --prefix apps/web run test:browser`) and the web unit,
  lint and build tiers: nothing in `apps/web` changed yet.
- `tests/unit` reports 56 errors on this machine. They are
  `PermissionError: [WinError 5]` on `AppData/Local/Temp/pytest-of-synce` at
  fixture setup, they are not caused by this work, and `main` at `bce6c61`
  reports the same 56. Counted rather than described: main 1857 passed /
  56 errors, this branch 1901 passed / 56 errors.

## What deliverable 4 has to decide first

Not a coding task with an obvious shape, which is why it is the checkpoint
rather than a loose end. Scope item 4 says:

> `lib/apiToken.ts` stays the one home of a browser-held credential; a second
> storage key is the drift this plan must not introduce.

and ADR-0005 §2 says the access token is held **only in JavaScript memory**,
never in `sessionStorage`. `apiToken.ts` today *is* `sessionStorage`, by
WEB-022, for the operator token. So the two credentials cannot share a storage
mechanism, and the scope item forbids them having two homes. The reconciliation
is the first thing to decide, and the plausible answer is that `apiToken.ts`
becomes the one *module* -- one accessor every screen calls -- holding a
session token in a module-level variable and an operator token in
`sessionStorage` behind the same interface, with the ADR's retirement question
("unchanged or deliberately retired") answered explicitly rather than by
default. That is a WEB- catalog entry and a paragraph in
`WEB_FIRST_WAVE_HANDOFF.md`, not just a component.

The rest of it is ordinary: a sign-in control that calls
`POST /api/v1/auth/sign-in` and navigates; a callback route that reads `code`
and `state`, posts them, and `history.replaceState`s them away before anything
else; a refresh-on-401 path that calls `/auth/refresh` once and retries; and
sign-out. `API_OIDC_REDIRECT_URIS` must name the callback route's URL exactly.

## What this still cannot finish here, unchanged

Implementation needs an **OIDC client registration and secret** from the Google
Cloud console, which no agent container holds. Everything above was built and
verified without one: the provider's network is faked and nothing else is, so
the ID tokens are really signed and really refused. What cannot be done here is
*shipping* it -- the exact-match redirect allowlist needs a stable origin, and
`deployment-smoke-target` records that there is not one yet.



### The value the ADR left open is now filled in

ADR-0005 §1 committed to "a single third-party OIDC provider" and deliberately
named none. It is **Google**, recorded 2026-09-20 in §1, *The provider, named*,
with GitHub ruled out on protocol grounds and an identity broker deferred.

### What an agent claiming this still cannot finish alone

Implementation needs an **OIDC client registration and secret** from the Google
Cloud console. That is a credential no agent container holds, and it is the
same shape of blocker `docs/plans/README.md` describes: build the work, but a
criterion that needs the real client is not satisfied by an unavailable
environment.

Google permits `http://localhost` redirect URIs, so the authorization-code
flow, the `state`/`nonce`/JWKS refusals, and every denial-path test can be
built and run before any deployment exists. What cannot be done here is
shipping it: the exact-match redirect allowlist ADR-0005 §1 requires needs a
stable origin, and `deployment-smoke-target` records that there is not one yet
and that this is deliberate.

## Why

The platform's write paths work and nobody can reach them. Saved analyses
(ADR-0003) and evidence packets (ADR-0004) are owner-scoped, validated at
write, versioned against concurrent edits, and reachable only with a token an
operator mints by hand and a reader pastes into a form. The web app is honest
about the consequence — `lib/savedAnalysis.saveDestination` picks the browser's
local store when no token is held, and says so on the control — but "saved in
this browser" is where a reader's work currently ends.

`docs/reference/WEB_FIRST_WAVE_HANDOFF.md` lists "account self-registration,
password flows, and session management beyond presenting an
operator-provisioned bearer token" among the first wave's explicit non-goals,
deliberately not stubbed. This plan is that non-goal becoming goal, and the
handoff's phrasing is the constraint: what ships here is identity, not a social
graph.

## Scope

The exact shape is the ADR's to decide; this plan implements it. What follows
is the scope boundary, not a substitute decision.

**In scope**

1. **Account lifecycle in `app_api`.** Registration, credential verification,
   and the schema additions the ADR calls for, provisioned by the reviewed
   bootstrap script beside the existing `api_app_writer` grants
   (`scripts/provision_app_api.py`). The warehouse role stays read-only and
   gets nothing here.
2. **Credential issuance behind the existing boundary.** Whatever a visitor
   presents at sign-in resolves to the same authenticated principal
   `apps/api/auth.py` already produces, so every owner-scoped route keeps its
   current authorization code and its current denial paths.
3. **Bounds on the first unauthenticated write.** Registration and sign-in are
   rate-limited per the ADR through `apps/api/ratelimit.py`, and neither
   credential nor account content reaches a log, a cache key, a response, or
   an error message — the rule `auth.py` already documents for tokens.
4. **The web sign-in surface.** Registration and sign-in screens, and the
   session or token they establish, replacing paste-a-token as the primary
   path. `lib/apiToken.ts` stays the one home of a browser-held credential;
   a second storage key is the drift this plan must not introduce.
5. **Migration.** Existing operator-provisioned accounts keep working, or are
   migrated deliberately, per the ADR.
6. **Contract documentation.** `docs/reference/API_CONSUMER_GUIDE.md`,
   `TESTING_CONTRACT.md` (the API and frontend ranges continue from API-139
   and WEB-104), and `CI_EVIDENCE_MAP.md`.

**Out of scope**

- Publishing, sharing, comments, follows, or any public artifact. That is
  `publishing-approval-path`, which depends on this plan.
- Any change to the public analytical surface. Anonymous reading must stay
  exactly as anonymous, as cacheable, and as unauthenticated as it is now.
- Roles, teams, or multi-tenant theming.
- Any warehouse change whatsoever.

## Acceptance criteria

- [x] A visitor can register, sign in, save an analysis and an evidence packet
      to their own account, sign out, sign in again, and find their work.
- [x] Every existing denial path still holds under the new credential:
      cross-user access answers `404`, enumeration is impossible, revoked
      credentials fail, and `409` still refuses an overwrite of an unread
      version.
- [x] Anonymous public reads are unchanged: same routes, same cache headers,
      same absence of identity, proven by the existing sweeps rather than by
      assertion.
- [x] No credential, account identifier, or account content appears in logs,
      cache keys, telemetry, URLs, referrers, or error text, proven by tests
      in the shape `tests/integration/api` already uses for tokens.
- [x] Registration and sign-in are bounded, and the bound is tested.
- [x] Deleting an account does what the ADR says, in one transaction, with
      evidence.
- [ ] The browser holds one credential in one place, and the local store's
      role as the signed-out destination is unchanged or deliberately retired.

## Validation

The plan's frontmatter commands, plus the browser tier
(`npm --prefix apps/web run test:browser`) for the sign-in surface. Denial-path
and isolation tests are the evidence that matters here; a passing happy path
proves almost nothing about an authentication change.
