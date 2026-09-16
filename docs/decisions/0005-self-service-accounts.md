# ADR-0005: Self-service accounts and the identity contract

- **Status:** Accepted
- **Date:** 2026-09-15
- **Accepted:** 2026-09-16 (human review, at
  [`docs/plans/gates/SELF_SERVICE_IDENTITY_GATE.md`](../plans/gates/SELF_SERVICE_IDENTITY_GATE.md)).
  Revised before acceptance after reviewer feedback: the credential is a
  third-party OIDC provider rather than an emailed sign-in link, and the
  browser holds a short-lived access token in memory beside an `HttpOnly`
  refresh cookie scoped to one path, rather than a token in `sessionStorage`.
- **Decision owners:** API platform maintainers
- **Related work:** `self-service-accounts` and, behind it,
  `publishing-approval-path` in [`docs/plans/to_do/`](../plans/to_do/);
  supersedes part of [ADR-0003](0003-saved-analysis-authentication-and-persistence.md)
- **Related decisions:** [ADR-0001](0001-data-layer-boundaries.md),
  [ADR-0003](0003-saved-analysis-authentication-and-persistence.md),
  [ADR-0004](0004-evidence-packet-persistence.md)

## Context

`AGENTS.md` opens: "This repository is the foundation for a public-data
analytics website and social hub." Every write path the platform has runs on a
credential only an operator can mint. `scripts/provision_app_api.py` says so in
its own docstring:

> There is no self-service signup by design: the consumers are this project's
> own web application and its operators, and an operator-gated credential is
> the smallest identity surface that supports user-owned storage honestly.

That was right when it was written. It is now the binding constraint. A visitor
who cannot obtain a credential cannot save an analysis (ADR-0003), cannot own
an evidence packet (ADR-0004), and cannot be the subject of any social feature.
Both of those contracts are already written in terms of an `owner_user_id` that
only an operator can create.

ADR-0003 anticipated this exactly, and deferred rather than foreclosed:

> No self-service signup, no passwords, no OAuth in this iteration: the
> consumer is the project's own web application and its operators, and every
> deferred alternative (sessions, OIDC) can be added behind the same
> `Authorization` boundary later without moving stored data.

It also set the precedent for **how** the question is reopened. API-007 was
forbidden from starting until an authentication, authorization, ownership,
privacy, retention, and deletion contract was approved by a human. The same
question is open again with a larger blast radius — a stranger's credential,
account recovery, a public identity, and abuse are all new — so it gets the
same treatment, and this document is the decision rather than the
implementation.

### Constraints inherited, quoted rather than paraphrased

These are not restated in this ADR's own words, because a paraphrase is how a
constraint quietly loosens.

**The warehouse stays read-only.** ADR-0003:

> The warehouse role (`api_reader`) is read-only and must stay that way;
> API-owned persistence uses separately owned tables and privileges (plan,
> "Stable warehouse boundary").

`sql/bootstrap/002_app_api.sql` states the same boundary at the schema:

> This schema is NOT warehouse content: it holds user-owned application data,
> it is absent from the warehouse manifest, and no ETL process reads or writes
> it. It exists so user-scoped persistence never requires granting the public
> serving role any mutation right -- `api_reader` stays read-only over the gold
> schemas and receives nothing here.

Nothing in this ADR adds a warehouse object, a warehouse grant, or an ETL
reader of `app_api`. Identity is `app_api` content, written by
`api_app_writer`, exactly as saved analyses and evidence packets already are.
Per ADR-0001's ownership table, this is Serving-layer and application state; it
is not Raw, Control, Silver, or Gold, and it holds no source facts.

**Private responses never enter the public cache.** ADR-0003:

> Private or user-specific responses are never stored in the shared public
> cache (plan, "Reliability, security, and operations"). The API-006 cache
> covers only the public analytical GET prefixes, and request telemetry logs
> no headers or parameter values, so tokens and private content have no
> existing path into caches or logs — this contract keeps it that way.

The mechanism is already correct by construction.
`RedisResponseCacheMiddleware._is_cacheable` caches only a `GET` whose path
`self.targets.covers(...)`, and the default is empty:

> Nothing is cacheable until the application declares what is. An empty
> default caches nothing rather than guessing, so a caller that forgets to
> pass targets loses an optimization instead of caching a resource nobody
> classified.

Every route this ADR proposes is therefore uncacheable unless somebody adds it
to `CacheTargets`, and every one of them additionally answers
`Cache-Control: private, no-store`, as the configuration routes already do.

**Telemetry stays route-shaped.** `apps/api/telemetry.py`:

> What is deliberately absent from the log line matters as much as what is in
> it: no query-string values (parameter values are user input and never belong
> in logs by default), no headers, no body, and nothing derived from the
> database URL.

A credential, an email address, and a one-time code are all request content.
None of them may appear in a log line, a cache key, a response, an error
string, or a URL. This is the rule that decides several details below — most
visibly, why a sign-in code is never carried in a query string.

**Ownership is scoped in SQL, not filtered afterwards.** ADR-0003:

> Every configuration row carries `owner_user_id`. All reads and writes are
> scoped by the authenticated owner in SQL, not filtered after the fact.
> Another user's configuration id answers `404` — indistinguishable from
> "never existed", so ids cannot be enumerated across users.

Self-service registration multiplies the number of owners from a handful of
operator-issued accounts to every visitor, which makes this rule more load-
bearing rather than less. It is unchanged.

**The social non-goal.** ADR-0003:

> whatever identity ships here must not overreach into a social account system
> it cannot yet justify.

This ADR takes that seriously and is deliberately narrower than "accounts"
usually implies — see *What an account is allowed to be*.

## Decision

### 1. Registration and credential: a third-party OIDC provider, and no secret of our own

**A visitor signs in with an existing account at a single third-party OIDC
provider.** The platform runs no password, sends no mail to authenticate, and
holds no credential a leak of its database could present anywhere.

What the database stores for identity:

- `(issuer, subject)` — the provider's stable identifier for that person,
  unique together. This is the account's identity and the only thing sign-in
  matches on.
- the provider's email claim, **stored only when the provider marks it
  verified**, for security-incident contact and for future notifications. An
  unverified claim is discarded rather than stored, because an unverified
  address is an assertion about someone else's mailbox.
- nothing else. No password hash, no recovery codes, no profile fields
  harvested from the provider.

The flow is the authorization-code flow with PKCE. The specifics are named
because each is a way to get this wrong: a `state` parameter bound to the
caller's session, a `nonce` echoed in the ID token, an exact-match redirect-URI
allowlist, ID-token signature verification against the provider's JWKS with
issuer, audience and expiry all checked, and a bounded clock skew. A library
does this; the implementing plan uses one and tests the refusals rather than
writing the protocol by hand.

**Recovery is the provider's, and that is the point.** Account recovery is a
hard problem with real support costs and real takeover risk, and a provider
that does it for millions of accounts does it better than this project would.
There is no recovery path here to build weaker than the front door, because
there is no path here at all.

**One provider at launch, and accounts are never auto-linked by email.** If a
second provider is added later, a visitor who signs in with a new provider gets
a new account, and connecting it to an existing one requires signing in with
the original first. Merging on a matching email claim is the standard shape of
this bug: a provider that asserts an address it never verified would take over
the account that owns it. `(issuer, subject)` is the identity; the email is
contact information, never a key.

**Recognised, deliberate costs.** Signing in to a public-data site becomes
conditional on holding an account with a particular company, which excludes
some visitors outright and tells that company which of its users read this
site. The front door also inherits the provider's availability — though a
signed-in reader with a live session is unaffected by an outage, which is what
§2's session lifetime buys. These are the reasons this was a close call
against an emailed sign-in link; see *Rejected alternatives*.

### 2. Session versus token: one boundary, a short-lived token in memory, and a refresh cookie scoped to one path

**ADR-0003's `Authorization: Bearer` boundary is preserved for every resource
route.** No route accepts a cookie as proof of identity. What changes from
ADR-0003 is only where the browser keeps the credential between requests.

A completed sign-in mints two things:

- an **access token**, opaque and short-lived (15 minutes), returned in the
  response body and held **only in JavaScript memory**. It is never written to
  `sessionStorage`, `localStorage`, or anywhere else that survives the page.
  Every authenticated request presents it as `Authorization: Bearer <token>`,
  so `apps/api/auth.py::require_account` keeps hashing what was presented and
  comparing digests in constant time.
- a **refresh token**, in a cookie marked `HttpOnly`, `Secure`,
  `SameSite=Strict`, and `Path=/api/v1/auth/refresh`. It is not readable by
  script and is not sent to any other path.

This is chosen over keeping the credential in `sessionStorage`, which is what
the operator token does today. `HttpOnly` does not stop script injected on this
origin from *acting* as the reader — the cookie rides along on requests that
script makes. What it stops is **exfiltration**: the attacker cannot lift the
credential and reuse it later from somewhere else. That downgrades a
successful XSS from a permanent account compromise to abuse bounded by the
page's lifetime, and the access token in memory dies with the tab and expires
in fifteen minutes regardless. `apps/web/lib/apiToken.ts` already states the
discipline this extends:

> Never into a URL, a link, a referrer, or history. A token in a query string
> travels into server logs and shared links; it reaches the API only as an
> `Authorization` header.

**What stops a cross-site request from spending the refresh cookie.** It is an
ambient credential, so this must be answered rather than assumed, and four
things answer it together:

1. `SameSite=Strict` — the browser does not attach it to any request initiated
   from another site, including top-level navigations.
2. `Path=/api/v1/auth/refresh` — it is not attached to any other route, so the
   entire CSRF surface is one endpoint rather than every mutating route. This
   is the property that makes a cookie acceptable here at all.
3. An `Origin` / `Sec-Fetch-Site` check on that one endpoint, refusing anything
   not same-origin. `infra/web/nginx.conf` serves the application at `/` and
   proxies `/api/` to the API on the same origin, so same-site is the real
   deployment topology and not an assumption.
4. Refresh **rotation with reuse detection**: each refresh returns a new
   refresh token and invalidates the old one. A second use of an already-spent
   token means it was captured, so the whole session family is revoked
   immediately.

A session lasts 30 days of inactivity with an absolute ceiling of 90 days
since sign-in. Revocation is stamping `revoked_at`, exactly as today, and
"sign out everywhere" revokes every credential for the account in one
statement.

```sql
-- illustrative; the implementing plan owns the real DDL
CREATE TABLE IF NOT EXISTS app_api.account_credential (
    credential_id   BIGSERIAL PRIMARY KEY,
    user_account_id BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('operator', 'access', 'refresh')),
    session_family  UUID,                 -- NULL for 'operator'; shared by a session's tokens
    token_sha256    TEXT NOT NULL UNIQUE,
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,          -- NULL for 'operator'
    revoked_at      TIMESTAMPTZ
);
```

Only the digest is ever stored, for every kind, which is ADR-0003's rule
unchanged. `session_family` is what makes reuse detection able to revoke a
compromised session without touching the reader's other devices.

**Failure text does not change.** `apps/api/auth.py`:

> The failure text never distinguishes "no such token" from "revoked token" --
> either would let a holder of a cancelled credential probe account state.

The same discipline applies one level up: the sign-in callback never reveals
whether an account already existed for the identity it just authenticated. A
response that distinguishes "welcome back" from "welcome" at the API layer
would be an oracle for whether a given person uses this site.

**Cost, stated plainly.** This is more machinery than a token in
`sessionStorage`: rotation, reuse detection, and the race where two tabs
refresh at once and one presents a token the other just spent. That race is
why reuse detection must key on the family and tolerate a short grace window
on the immediately-previous token, rather than treating every double-use as an
attack and logging people out for using two tabs.

### 3. What an account is allowed to be: private by default, named only on publishing

This is where ADR-0003's non-goal is honoured rather than quietly outgrown. An
account is **not** a profile. There is no directory, no search over accounts,
no follower graph, no activity feed, and no way for one account to learn that
another exists. Those belong to plans that do not exist yet, and inventing
them here would be the overreach ADR-0003 refused.

Two identities, deliberately separate:

- **Private identity** — `(issuer, subject)`, the verified email if the
  provider gave one, and `user_account_id`. Never rendered to any other
  account, ever, under any feature in this ADR.
- **Public display name** — nullable, absent until the account first publishes
  something, and chosen at that moment rather than at registration. Nothing
  from the provider is used as a default: a provider's display name or
  username is that company's identifier for a person, not a name they chose to
  publish under here.

`app_api.user_account.display_label` is `NOT NULL` today and is an **operator
label**, not a public name — `provision_app_api.py --issue-token LABEL` sets
it and `Account.display_label` carries it. A self-service account gets a
system-generated opaque value there, and the public name lands in a new
nullable `public_display_name` column. Reusing `display_label` for a
stranger-visible name would silently publish operator labels that were written
on the assumption nobody outside the deployment would read them.

A public display name is 3–32 characters, unique case-insensitively (so one
account cannot dress as another), and changeable; the previous name is
released for reuse.

**What a reader of a published artifact sees:** the display name **as it stood
when that artifact was published**, snapshotted onto the published row rather
than joined live from the account. Two reasons. A live join means renaming an
account silently rewrites the attribution on everything it ever published,
including things other people have cited. And a live join makes deletion a
dangling reference, where a snapshot lets the published row be destroyed
outright with nothing left pointing at a person (§5). Publishing itself —
approval, visibility, unpublishing — is `publishing-approval-path`'s decision,
not this one; this ADR fixes only *what identity is attached* when that plan
attaches one.

**ADR-0003's sharing rule is not superseded.** It says:

> There are no shared or public configurations in this iteration; sharing is
> future social scope.

That remains true after this ADR. Accounts alone publish nothing. The rule
changes in `publishing-approval-path` or not at all.

### 4. Abuse and cost

Everything unauthenticated the platform accepts today is a read over a
read-only role. The sign-in start and callback routes are the first
unauthenticated writes, and account creation on first successful callback is
the first row a stranger can cause to exist.

**Delegating the credential also delegates most of the abuse problem.**
Creating an account here requires completing a real authorization flow with a
real provider account, and providers run their own abuse prevention at a scale
this project never will. There is no mail to send, so this service cannot be
turned into a way to bother a third party's inbox — the vector an emailed
sign-in link would have introduced, and a reason this shape is cheaper to
defend as well as cheaper to operate.

What remains is bounded explicitly:

**A third rate-limit bucket, `identity`.** `apps/api/ratelimit.py` has two:

> Two token buckets per client: ``catalog`` for the inexpensive discovery
> reads and ``analysis`` for everything that reaches observation or analysis
> SQL. The split is the plan's requirement stated directly — a client browsing
> the catalog must not spend the budget that protects the expensive queries,
> and vice versa.

The same argument gives identity its own bucket: a reader signing in must not
be throttled by their own chart browsing, and a callback flood must not be
payable out of the analysis budget. It is set far tighter than either, because
a human signs in rarely and a script does not.

Beside it:

- **A global ceiling on account creation per hour** across the deployment, so
  an attacker with many provider accounts degrades into a queue rather than an
  unbounded row count. Exceeding it answers the same stable `429` shape with
  `Retry-After` the limiter already answers.
- **Per-account storage quotas** on saved analyses and evidence packets, so a
  single account cannot consume the database. ADR-0004 already bounds a single
  packet; this bounds how many.

Two properties of the existing limiter are inherited and must be stated, not
rediscovered:

> State is in-process. The deployment runs a single API process, and the
> limiter protects the database behind it; a multi-process deployment would
> multiply the budget by the worker count, which is recorded rather than
> hidden.

and

> The client is the TCP peer, unless the peer is a proxy the deployment
> declared in ``API_TRUSTED_PROXY_IPS`` -- then it is the address that proxy
> forwarded (API-075). [...] A forwarded address is read only from a declared
> hop: taken unconditionally it would be worse than ignored, because a direct
> client could mint a fresh budget per request by varying a header it
> controls.

For catalog reads a multiplied budget is a tuning error. For the identity
routes it is the difference between a bound and no bound, so **the identity
routes require `API_TRUSTED_PROXY_IPS` to be configured**; unconfigured,
per-client identity limiting is one budget for the entire internet and the
deployment should be treated as having none.

**What an operator can do.** Everything they can do today, unchanged: stamp
`revoked_at` on a credential and it stops working immediately. Added at the
account level: block an identity from signing in again, and revoke every live
session for an account in one statement. Both are `app_api` writes available
to `provision_app_api.py`, which keeps being the reviewed, manual, privileged
path — this ADR does not propose an admin UI, and an operator action remains a
deliberate act rather than a button.

### 5. Privacy, retention, deletion, and export

ADR-0003's answers are extended to the account itself rather than replaced:

> Configurations are kept until their owner deletes them — deletion is a hard
> `DELETE`, effective immediately, and answered idempotently. `GET` of a
> configuration is its own export (the document is the user's content,
> returned verbatim). Revoking an account deletes its token; deleting an
> account deletes its configurations in the same transaction. No analytics or
> derived retention of user content.

**Everything the platform holds about a person**, exhaustively: the
`(issuer, subject)` pair, a verified email if the provider supplied one, the
timestamps on their credentials, and the content they created. No IP log, no
device fingerprint, no analytics profile, no third-party tracker, and nothing
harvested from the provider beyond the two items named. The telemetry rule
quoted in *Context* already forbids the request-level half of that, and this
ADR adds no exception to it.

**Deletion is a hard `DELETE` of the account row**, immediate and idempotent,
cascading to credentials, saved analyses, evidence packets, and published
artifacts. The cascade is already in the schema — `ON DELETE CASCADE` on both
`saved_analysis_configuration.owner_user_id` and
`evidence_packet.owner_user_id` — so this is the existing mechanism reaching
one level up, not a new one.

**Deletion removes published artifacts, and this is deliberate.** An artifact
published by a deleted account stops being served and answers the `404` any
unknown id answers, even where people were reading it. The alternative —
keeping the artifact with its author redacted to "deleted account" — was
rejected because it leaves a person who asked to be forgotten as the author of
content the platform keeps serving. A platform that cannot honour "delete my
account" without an asterisk should not offer the button. Because the display
name is snapshotted onto the published row (§3) rather than joined, deleting
the account destroys the name with the row and leaves nothing to redact.

**Deletion propagates to backups within their retention window.** A hard
`DELETE` clears the live database, but point-in-time-recovery snapshots still
contain the row, and a deletion promise that quietly expires at the backup
boundary is not one. The contract: the deployment declares a backup retention
window, deleted data is gone from production immediately and from every
retained backup once that window has passed, and a restore performed inside
the window re-applies the deletion log before the database serves traffic. The
implementing plan owns the mechanism; this ADR fixes that the promise covers
backups and that the window is a published number rather than an accident of
configuration.

**What deletion cannot promise.** The platform can stop serving an artifact;
it cannot recall a copy. Anyone who exported, screenshotted, or cited it keeps
what they have, and a search engine may hold a cached copy for a while.
Deletion is a promise about this platform's future behaviour, not about the
past, and it is worded that way to a reader rather than implied.

**Deletion requires a fresh proof of identity** — a sign-in completed within
the last 10 minutes. A 30-day session is a convenience for saving charts; it
is not sufficient authority to destroy everything an account owns from an
unattended laptop. It is immediate once confirmed, with no grace period and no
soft-delete state: a "deleted" row awaiting a purge is exactly the residue
this section exists to refuse.

**Account-level export, because per-resource `GET` is not one.** ADR-0003's
"`GET` is its own export" is true per configuration and useless to someone who
wants their work back and does not know their own ids. A single authenticated
`GET` returns one document containing everything the first paragraph of this
section lists, verbatim. It is `private, no-store` like every other account
route, and it is the answer to "let me leave" that makes immediate hard
deletion defensible rather than punitive.

### 6. Migration: existing tokens keep working, and are not invalidated

There are operator-provisioned tokens in use today, held by the project's own
web application and its operators. **They continue to work unchanged, with no
expiry and no forced migration.** Concretely:

- Every existing `app_api.user_account` row becomes an account with one
  `kind = 'operator'` credential carrying its current `token_sha256`,
  `created_at`, and `revoked_at`. The digest is copied, not regenerated — the
  tokens in circulation are the same tokens.
- `issuer`, `subject`, `email` and `public_display_name` are all nullable, so
  an operator account has none of them and is valid without them. An operator
  account cannot sign in through a provider until somebody links an identity
  to it, which is an operator action and never automatic.
- `scripts/provision_app_api.py --issue-token` and `--revoke-token-label`
  keep working and keep meaning what they mean. Re-running
  `sql/bootstrap/002_app_api.sql` remains the migration mechanic, as it was
  for ADR-0004's table:

  > Placing the `CREATE TABLE` above that block means a fresh bootstrap is
  > correct, and re-running the whole file against a deployed database is the
  > migration — every statement in it is already idempotent.

- `apps/api/auth.py::require_account` changes where it looks up a digest —
  the credential table rather than `user_account` — and gains an expiry
  check. Its contract does not change: same header, same constant-time
  compare, same undifferentiated `401`, same `503` when app storage is
  unconfigured.

Nothing about the public analytical surface changes. No warehouse object, no
warehouse grant, no new role, and no new engine: `api_app_writer` already owns
`app_api` and is the only role that writes any of this.

## Rejected alternatives

- **An emailed single-use sign-in link.** The strongest alternative, and the
  one this ADR originally proposed. It keeps identity first-party, needs no
  third party, and makes recovery identical to sign-in. Rejected on failure
  modes rather than on design: it makes outbound mail a hard dependency of the
  front door, and mail is the least reliable channel available. A new sending
  domain has no reputation, spam placement is silent to the operator and
  indistinguishable from an outage to the reader, delivery latency races a
  short link expiry, and the failure lands on first sign-in — the worst
  possible moment. It also introduces a mail-bomb vector that needs its own
  per-address bound, and trains readers to click sign-in links in email.
  **This remains the substitution to make if the third-party dependency is
  judged worse than the mail one; §2 onward is unaffected by the swap.**

- **Email plus password.** Does not remove the mailbox as the single factor —
  it adds a secret *on top of* a mailbox-based reset path. The platform would
  store a password hash it did not need, inherit credential-stuffing from
  reuse elsewhere, and still send exactly as much email. Strictly more surface
  for no additional security property.

- **The credential in `sessionStorage`**, as the operator token is held today.
  Simplest, with no cookie and therefore no CSRF surface at all, and it was
  this ADR's original proposal. Rejected because script running on this origin
  can read it and exfiltrate it, turning a successful XSS into a permanent
  account compromise rather than a bounded one. The hybrid in §2 keeps the
  single `Authorization` boundary for every resource route while putting the
  long-lived credential out of script's reach, and confines the CSRF surface
  it takes on to one path.

- **A plain `HttpOnly` session cookie** with no access token. Simpler than the
  hybrid and still unreadable by script, but the cookie becomes an ambient
  credential on *every* route, so `SameSite` and an origin check would have to
  be correct on every mutating endpoint rather than on one. It would also sit
  beside ADR-0003's `Authorization` boundary rather than inside it, and every
  route would have to accept either — which is where authorization bugs live.

- **Signed stateless session tokens (JWT).** Rejected for the reason ADR-0003
  already gave, which applies with more force to sessions that must be
  revocable from a "sign out everywhere" button and from reuse detection:
  "revocation would need a denylist table anyway; opaque hashed tokens are
  simpler and strictly easier to revoke."

- **Anonymous accounts with no external identity at all** (a credential
  printed once, like today's operator flow, but self-service). Maximally
  private, and rejected because it makes saved work disposable by
  construction: losing the printed credential loses the account with no path
  back.

- **Keeping published artifacts with the author redacted on deletion.**
  Rejected in §5: it leaves a person who asked to be forgotten as the author
  of content the platform keeps serving.

- **A profile, a directory, or a display name at registration.** Rejected as
  the overreach ADR-0003 named. An account that only its owner can observe is
  the smallest thing that unblocks saved analyses and evidence packets.

## Consequences

**For `self-service-accounts`** (the implementing plan): the OIDC client and
its callback, the credential table and the migration of existing rows into it,
access/refresh issuance with rotation and reuse detection, the `identity`
rate-limit bucket and the account-creation ceiling, per-account storage
quotas, account export, account deletion with a freshness requirement, and the
backup-purge mechanism §5 commits to. Denial-path tests are the point of the
plan, not a garnish: a tampered `state`, a replayed `nonce`, an unregistered
redirect URI, an ID token with a bad signature or a wrong audience or an
expired `exp`, an expired access token, a revoked session, a reused refresh
token revoking its family, two tabs refreshing concurrently *not* revoking
anything, cross-account access, and proof that no token, code, or address
reaches a log line, a cache key, or an error body.

**For `publishing-approval-path`:** it inherits `public_display_name`, the
snapshot-at-publish rule, and the delete-on-account-deletion rule, and it owns
everything else about publishing.

**For the deployment:** an OIDC client registration and its secret; one
configuration value that stops being optional (`API_TRUSTED_PROXY_IPS`, §4); a
declared backup retention window (§5); and **no mail dependency** — the stored
email is contact-of-last-resort, and sending to it is a future need rather
than a precondition for sign-in. No new role, schema, engine, or warehouse
object. `BETA_RESET_REINGESTION.md` gains the re-run of `002_app_api.sql`,
which it already describes as the migration mechanic.

**For readers:** a visitor can hold an account without inventing another
password, keep their own work, get all of it back in one request, and destroy
all of it in one request. What the platform knows about them is a provider
identifier, possibly an email address, and what they chose to create — and
what it keeps after deletion is nothing.

**Superseded:** ADR-0003's sentence "No self-service signup, no passwords, no
OAuth in this iteration" — self-service signup through OIDC is now in scope,
by the mechanism ADR-0003 itself pointed at. ADR-0003's implicit assumption
that the browser credential lives in `sessionStorage` is narrowed by §2: the
`Authorization` boundary is unchanged, but a self-service session keeps its
long-lived half in an `HttpOnly` cookie. Nothing else in ADR-0003 or ADR-0004
is superseded; ownership scoping, optimistic concurrency, the
`private, no-store` answer, hard deletion, the read-only warehouse boundary,
and the sharing non-goal all survive this ADR intact.

**Corrected in passing:** ADR-0003 says "revocation is deleting the token
row", but `apps/api/auth.py`, `app_api.user_account`, and
`provision_app_api.py --revoke-token-label` all stamp `revoked_at` instead:
"Stamps ``revoked_at``. The credential stops working immediately; the
account's configurations are left intact until the account is deleted."
Stamping is the better behaviour — it keeps an audit trail and prevents a
digest being reissued — and this ADR records it as the contract. Deleting the
row is what account deletion does, which is a different act with a different
consequence.
