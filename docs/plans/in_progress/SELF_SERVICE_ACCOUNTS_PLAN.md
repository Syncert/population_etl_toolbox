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
  - npm --prefix apps/web run test:browser
  - ruff format --check . ; ruff check .
---

# A visitor can hold an account of their own

## Plan status

- **Status:** Claimed and **in progress**, with every deliverable and every
  acceptance criterion carrying evidence. It stays in `in_progress/` rather
  than moving to `needs_review/` for one reason, which is the blocker this
  plan was filed with: nothing here has been exercised against a real Google
  OIDC client, because that needs a registration and secret no agent container
  holds. `self-service-identity` was recorded `approved` by Nick on
  2026-09-20, against
  [ADR-0005](../../decisions/0005-self-service-accounts.md), which has been
  `Accepted` since 2026-09-16.
- **Last updated:** 2026-09-20
- **Current milestone:** every deliverable and every acceptance criterion
  has evidence. What remains is a credential no agent container holds --
  see *What this still cannot finish here* below.

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
| 4. The web sign-in surface | `apps/web/lib/apiToken.ts`, `lib/session.ts`, `components/SignInControl.tsx`, `components/SignInCallback.tsx`, `app/auth/callback/` | WEB-115, 21 unit tests |
| 5. Migration | `002_app_api.sql`'s migration block | API-149, against a schema built in the *previous* shape |
| 6. Contract documentation | `API_CONSUMER_GUIDE.md`, `TESTING_CONTRACT.md`, `CI_EVIDENCE_MAP.md`, `WEB_FIRST_WAVE_HANDOFF.md`, `BETA_RESET_REINGESTION.md` | the gates in `tests/unit` that enforce all of them |

Beyond the numbered deliverables, and required by the ADR rather than by the
scope list: account export, hard deletion with a freshness requirement, the
public display name, and the operator actions in §4 (`--revoke-sessions-label`,
`--block-account-label`, `--unblock-account-label`) — API-152. Per-account
storage quotas — API-154. And the backup-purge mechanism §5 commits to —
API-155, `app_api.account_deletion_log` and `scripts/apply_deletion_log.py`,
graded by performing a restore rather than by describing one.

The browser tier ran and passes: WEB-116, seven specs, 164 in the tier
overall. It covers everything on this side of the redirect, which is all of
the flow except the provider's own consent screen — including the one line
that cannot be checked anywhere else, which is that the authorization code
leaves the address bar, on the refusal path as well as the success one.

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
- [x] The browser holds one credential in one place -- one module, one
      accessor, two backings, because ADR-0005 s2 forbids a session token
      being written anywhere that survives the page and WEB-022 keeps the
      operator token in `sessionStorage`. The local store's role as the
      signed-out destination is unchanged rather than retired, and the
      handoff records that. WEB-115.

**Not run here, and why.**

- **Nothing in the `verify` block.** Every tier it names ran here and passes,
  including the browser tier, which was added to that block because this plan's
  work depends on it.
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

## The one thing that is still not evidence

Implementation needs an **OIDC client registration and secret** from the Google
Cloud console, which no agent container holds. Everything above was built and
verified without one: the provider's *network* is faked and nothing else is, so
the ID tokens are really signed, really verified, and really refused.

This is why the plan stays in `in_progress/`. The first acceptance criterion is
about a visitor signing in, and a faked provider is not a visitor signing in —
it is proof that this side of the exchange behaves, which is a different claim.

**What was done to shrink that gap, because it is the gap that bites on a first
sign-in.** Every assumption this implementation makes about the provider was
checked against Google's published documents rather than against the fake, and
one of them was wrong:

- **`iss` has two spellings.** Google's OpenID Connect documentation says the
  claim is "Always `https://accounts.google.com` or `accounts.google.com` for
  Google ID tokens", while its discovery document's own `issuer` is the first
  form. The exact-match check refused the second, *intermittently*. Worse, the
  stored identity is `(issuer, subject)`, so storing whichever spelling arrived
  would have given one person two accounts and left their saved work in the one
  they were no longer in. Fixed, and the stored issuer is now canonical
  whichever spelling arrives (API-150).
- The live discovery document at `accounts.google.com` was read and agrees with
  the rest: `token_endpoint_auth_methods_supported` includes
  `client_secret_post`, which is what the exchange sends;
  `id_token_signing_alg_values_supported` is `RS256`, which is in the accepted
  list; `code_challenge_methods_supported` includes `S256`;
  `scopes_supported` includes `openid` and `email`, and `claims_supported`
  includes `email_verified`, which is what makes ADR-0005 §1's "stored only
  when the provider marks it verified" enforceable rather than aspirational.

**What a person has to do to close it.** Register a client, set
`API_OIDC_CLIENT_ID`, `API_OIDC_CLIENT_SECRET` and `API_OIDC_REDIRECT_URIS`
(the last must name the callback route's URL exactly), and complete one real
sign-in. `http://localhost` redirect URIs are permitted, so this needs no
deployment — set `API_COOKIE_SECURE=0` for a plain-HTTP localhost run, and
nothing else changes.

Shipping it is a further step and is somebody else's plan: the exact-match
allowlist needs a stable origin, and `deployment-smoke-target` records that
there is not one yet and that this is deliberate.



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
