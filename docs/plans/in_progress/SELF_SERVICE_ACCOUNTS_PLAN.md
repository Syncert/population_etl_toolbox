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

- **Status:** Unclaimed and **no longer gated.** `self-service-identity` was
  recorded `approved` by Nick on 2026-09-20, against
  [ADR-0005](../../decisions/0005-self-service-accounts.md), which has been
  `Accepted` since 2026-09-16.
- **Last updated:** 2026-09-20
- **Current milestone:** not started.

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

- [ ] A visitor can register, sign in, save an analysis and an evidence packet
      to their own account, sign out, sign in again, and find their work.
- [ ] Every existing denial path still holds under the new credential:
      cross-user access answers `404`, enumeration is impossible, revoked
      credentials fail, and `409` still refuses an overwrite of an unread
      version.
- [ ] Anonymous public reads are unchanged: same routes, same cache headers,
      same absence of identity, proven by the existing sweeps rather than by
      assertion.
- [ ] No credential, account identifier, or account content appears in logs,
      cache keys, telemetry, URLs, referrers, or error text, proven by tests
      in the shape `tests/integration/api` already uses for tokens.
- [ ] Registration and sign-in are bounded, and the bound is tested.
- [ ] Deleting an account does what the ADR says, in one transaction, with
      evidence.
- [ ] The browser holds one credential in one place, and the local store's
      role as the signed-out destination is unchanged or deliberately retired.

## Validation

The plan's frontmatter commands, plus the browser tier
(`npm --prefix apps/web run test:browser`) for the sign-in surface. Denial-path
and isolation tests are the evidence that matters here; a passing happy path
proves almost nothing about an authentication change.
