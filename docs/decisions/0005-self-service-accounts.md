# ADR-0005: Self-service accounts and the identity contract

- **Status:** Proposed
- **Date:** 2026-09-15
- **Accepted:** not yet — `docs/plans/gates/SELF_SERVICE_IDENTITY_GATE.md` is
  the human review this document exists to be judged by
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

### 1. Registration and credential: an emailed one-time link, and no password

**A visitor registers and signs in with an email address and nothing else.**
The platform emails a single-use sign-in link; following it proves control of
the mailbox and mints a session credential. There is no password, and
therefore no password to store, leak, reuse, phish at scale, or reset.

What the database stores for identity:

- the email address, case-folded, unique — the only personal datum the
  platform holds;
- for each outstanding sign-in attempt, `sha256(link_token)`, an expiry, and a
  single-use marker — never the token itself;
- nothing else. No password hash, no security questions, no recovery codes.

The link token is 256 bits of `secrets.token_urlsafe` entropy, valid **15
minutes**, usable **once**, and invalidated when a newer one is requested for
the same address. It is delivered only in the email body and is submitted to
the API in a request body — never as a query parameter, because a token in a
query string reaches server logs, `Referer` headers, and shared links. That is
the same discipline `apps/web/lib/apiToken.ts` already states for the bearer
token:

> Never into a URL, a link, a referrer, or history. A token in a query string
> travels into server logs and shared links; it reaches the API only as an
> `Authorization` header.

**The recovery path is the sign-in path.** This is the whole argument for the
shape. There is no separate "forgot" flow to build, to test, or to leave
subtly weaker than the front door, and there is no second secret whose loss
strands a user's saved work. A reader who still controls the mailbox can
always get back in; a reader who has lost the mailbox has lost the account,
which is stated plainly to them at registration rather than discovered later.

That last sentence is the honest cost, so it is written into the consequences
rather than buried: **the mailbox is the single factor.** Whoever controls it
controls the account and everything it owns. Mitigations that do not require
inventing a second secret: sessions are revocable individually and in bulk
(§2), a sign-in to an account with an active session notifies the address, and
account deletion is not reachable from a link-only session without a fresh
sign-in inside the last 10 minutes (§5).

**Recognised, deliberate cost: outbound email becomes a required deployment
dependency.** The platform has none today. Sign-in cannot work without a
deliverable path to a stranger's inbox, which means a mail provider, a
credential for it, SPF/DKIM alignment, and bounce handling. An address that
hard-bounces is marked undeliverable and stops being mailed, because a service
that keeps mailing a dead address is how a sending reputation dies. This is
new operational surface and the reviewer should weigh it against §*Rejected
alternatives*, where OIDC trades it for a third-party dependency instead.

### 2. Session versus token: sessions issue the same credential, they are not a second one

**ADR-0003's single `Authorization: Bearer` boundary is preserved exactly. No
cookie authenticates anything.**

A successful sign-in returns an opaque **session token** in the response body.
The web application holds it precisely where it holds the operator token
today — in memory, and in `sessionStorage` only when the reader asked this
browser to remember it — so `apps/web/lib/apiToken.ts` keeps its contract and
its comment. Every authenticated request still presents
`Authorization: Bearer <token>`, and `apps/api/auth.py::require_account` keeps
hashing what was presented and comparing digests in constant time.

Storage-wise a session is a credential row beside the operator ones, not a new
mechanism: same `sha256` at rest, same revocation by stamping `revoked_at`,
same refusal text. The existing `app_api.user_account` grows a credential
child table so one account can hold several live sessions — a phone and a
laptop — where an operator account holds exactly one long-lived token.

```sql
-- illustrative; the implementing plan owns the real DDL
CREATE TABLE IF NOT EXISTS app_api.account_credential (
    credential_id   BIGSERIAL PRIMARY KEY,
    user_account_id BIGINT NOT NULL
        REFERENCES app_api.user_account (user_account_id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('operator', 'session')),
    token_sha256    TEXT NOT NULL UNIQUE,
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,          -- NULL for 'operator'
    revoked_at      TIMESTAMPTZ
);
```

A session expires **30 days** after last use, with an absolute ceiling of
**90 days** since issue, after which a fresh sign-in is required. An operator
token has no expiry, which is what `kind` exists to distinguish.

**On cookies, and what would stop a cross-site request from spending one:**
nothing needs to, because there is no ambient credential. A cookie is attached
by the browser to any request the browser is tricked into making, which is why
a cookie session must be answered with `SameSite`, an anti-forgery token, or
both. A credential the application must read out of storage and place into a
header itself cannot be spent by a cross-origin form post or an `<img>` tag at
all. Choosing the header is choosing not to have the CSRF class.

The residual risk this leaves is **XSS**, and it is not hidden: script running
on the application's own origin can read `sessionStorage` and mint requests.
That risk exists identically today for the operator token, the application
already serves a CSP with a nonce (`apps/web/scripts/check-csp-nonce.mjs`
enforces it), and the honest comparison is that cookie sessions would trade
this exposure for the CSRF one rather than eliminating it. `HttpOnly` cookies
would genuinely beat `sessionStorage` on XSS — that is the one real argument
against this choice, and it is recorded in *Rejected alternatives*.

**Failure text does not change.** `apps/api/auth.py`:

> The failure text never distinguishes "no such token" from "revoked token" --
> either would let a holder of a cancelled credential probe account state.

Registration and sign-in inherit the same discipline one level up: requesting
a sign-in link answers **`202 Accepted` whether or not the address has an
account**. A response that distinguishes the two turns the endpoint into an
oracle for "does this person use this site", which is a privacy leak about
someone who never consented to be looked up.

### 3. What an account is allowed to be: private by default, named only on publishing

This is where ADR-0003's non-goal is honoured rather than quietly outgrown. An
account is **not** a profile. There is no directory, no search over accounts,
no follower graph, no activity feed, and no way for one account to learn that
another exists. Those belong to plans that do not exist yet, and inventing
them here would be the overreach ADR-0003 refused.

Two identities, deliberately separate:

- **Private identity** — the email address and `user_account_id`. Never
  rendered to any other account, ever, under any feature in this ADR. It is
  the login handle and nothing else.
- **Public display name** — nullable, absent until the account first publishes
  something, and chosen at that moment rather than at registration. Asking a
  stranger to pick a public name before they have anything public is how an
  email address ends up as a default display name.

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
dangling reference, where a snapshot lets the published row stand or fall on
its own terms (§5). Publishing itself — approval, visibility, unpublishing —
is `publishing-approval-path`'s decision, not this one; this ADR fixes only
*what identity is attached* when that plan attaches one.

**ADR-0003's sharing rule is not superseded.** It says:

> There are no shared or public configurations in this iteration; sharing is
> future social scope.

That remains true after this ADR. Accounts alone publish nothing. The rule
changes in `publishing-approval-path` or not at all.

### 4. Abuse and cost: registration is the first unauthenticated write

Everything unauthenticated the platform accepts today is a read over a
read-only role. Requesting a sign-in link is a write (a row, plus an email the
platform pays to send), triggerable by anyone, and directs traffic at a third
party's inbox — so it is simultaneously a database-cost problem, a money
problem, and a way to use this service to bother someone else.

**A third rate-limit bucket, `identity`.** `apps/api/ratelimit.py` has two:

> Two token buckets per client: ``catalog`` for the inexpensive discovery
> reads and ``analysis`` for everything that reaches observation or analysis
> SQL. The split is the plan's requirement stated directly — a client browsing
> the catalog must not spend the budget that protects the expensive queries,
> and vice versa.

The same argument gives identity its own bucket: a reader signing in must not
be throttled by their own chart browsing, and a registration flood must not be
payable out of the analysis budget. It is set far tighter than either — single
digits per hour per client — because a human signs in rarely and a script does
not.

Per-client limiting alone is insufficient here, so two bounds sit beside it,
keyed by the thing being attacked rather than by the attacker:

- **Per address:** at most a few outstanding sign-in links per address per
  hour, regardless of who asked. This is the one that stops the service being
  used to mail-bomb a stranger, and a per-IP bucket cannot do it.
- **Per account creation:** a global ceiling per hour across the deployment,
  so a distributed signup flood degrades into a queue rather than an unbounded
  row count and an unbounded mail bill. Exceeding it answers the same stable
  `429` shape with `Retry-After` that the limiter already answers.

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

For catalog reads a multiplied budget is a tuning error. For registration it
is the difference between a bound and no bound, so **the identity routes
require `API_TRUSTED_PROXY_IPS` to be configured**; unconfigured, per-client
identity limiting is one budget for the entire internet and the deployment
should be treated as having none.

**An unverified address holds nothing.** An account exists only once a link is
followed. A requested-but-never-completed sign-in is a short-lived row that
expires, not an account — so a signup flood costs rows that reap themselves
rather than permanent accounts.

**What an operator can do.** Everything they can do today, unchanged: stamp
`revoked_at` on a credential and it stops working immediately. Added at the
account level: block an address from obtaining new links, and revoke every
live session for an account in one statement. Both are `app_api` writes
available to `provision_app_api.py`, which keeps being the reviewed, manual,
privileged path — this ADR does not propose an admin UI, and an operator
action remains a deliberate act rather than a button.

### 5. Privacy, retention, deletion, and export

ADR-0003's answers are extended to the account itself rather than replaced:

> Configurations are kept until their owner deletes them — deletion is a hard
> `DELETE`, effective immediately, and answered idempotently. `GET` of a
> configuration is its own export (the document is the user's content,
> returned verbatim). Revoking an account deletes its token; deleting an
> account deletes its configurations in the same transaction. No analytics or
> derived retention of user content.

Extended:

- **Data held about a person** is the email address, the timestamps on their
  credentials, and the content they created. No IP log, no device
  fingerprint, no analytics profile, no third-party tracker. The telemetry
  rule quoted above already forbids the request-level half of that.
- **Deletion is a hard `DELETE` of the account row**, immediate and
  idempotent, cascading to credentials, saved analyses, and evidence packets.
  The cascade is already in the schema — `ON DELETE CASCADE` on both
  `saved_analysis_configuration.owner_user_id` and
  `evidence_packet.owner_user_id` — so this is the existing mechanism reaching
  one level up, not a new one.
- **Deletion requires a fresh proof of the mailbox**: a sign-in completed
  within the last 10 minutes. A 30-day session is a convenience for saving
  charts; it is not sufficient authority to destroy everything the account
  owns from an unattended laptop.
- **Account-level export, because per-resource `GET` is not one.** ADR-0003's
  "`GET` is its own export" is true per configuration and useless to someone
  who wants their work back and does not know their own ids. A single
  authenticated `GET` returns one document containing the account's email,
  creation date, every saved configuration, and every evidence packet,
  verbatim. It is `private, no-store` like every other account route, and it
  is the answer to "let me leave" that makes hard deletion defensible.

**What deletion does to content someone else is already reading.** This is the
question the gate singles out, and the answer is a choice with a real cost
either way.

**Decision: deleting an account unpublishes and deletes its published
artifacts.** A reader's link to a published artifact by a deleted account stops
resolving, and answers the same `404` that any unknown id answers. The
alternative — keeping the artifact and redacting its author to "deleted
account" — was rejected because it means a person who asked to be deleted
remains the author of content the platform keeps serving, and because the
published row would reference an `owner_user_id` that no longer exists,
turning the cascade above into a special case with a dangling pointer. A
platform that cannot honour "delete my account" without an asterisk should not
offer the button.

The limit of that promise is stated rather than implied: **the platform can
stop serving an artifact; it cannot recall a copy.** Anyone who already
exported, screenshotted, or cited the artifact keeps what they have, and a
search engine may hold a cached copy for some time. Deletion is a promise
about this platform's future behaviour, not about the past.

Because a snapshot name (§3) lives on the published row rather than being
joined from the account, deleting the account removes the name with the row
and leaves nothing to redact.

### 6. Migration: existing tokens keep working, and are not invalidated

There are operator-provisioned tokens in use today, held by the project's own
web application and its operators. **They continue to work unchanged, with no
expiry and no forced migration.** Concretely:

- Every existing `app_api.user_account` row becomes an account with one
  `kind = 'operator'` credential carrying its current `token_sha256`,
  `created_at`, and `revoked_at`. The digest is copied, not regenerated — the
  tokens in circulation are the same tokens.
- `email` and `public_display_name` are nullable, so an operator account has
  neither and is valid without them. An operator account cannot sign in by
  email until somebody attaches an address to it, which is an operator action
  and is never automatic.
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

- **Email plus password.** The credential every visitor expects, and the
  reason to reject it is that it does not remove the mailbox as the single
  factor — it adds a secret *on top of* a mailbox-based reset path. The
  platform would then store a password hash it did not need, inherit
  credential-stuffing from reuse elsewhere, and still send exactly as much
  email. Recovery would be a second flow with its own tokens and its own
  chances to be weaker than the front door. Strictly more surface for no
  additional security property, given that the reset path exists.

- **Third-party OIDC (sign in with GitHub/Google) now.** Genuinely
  attractive, and the closest call here: it needs no outbound mail, and its
  recovery is better than anything this project would build. Rejected for
  first delivery because it makes signing in to a public-data site conditional
  on holding an account with a particular company, tells that company which
  of its users read this site, and couples the platform's front door to a
  third party's availability and terms. ADR-0003's own sentence applies
  unchanged — it "can be added behind the same `Authorization` boundary later
  without moving stored data" — and §2 is designed so that adding it later is
  one more way to mint a session credential, not a second authentication
  mechanism. **If the reviewer prefers to trade the mail dependency for the
  third-party one, this is the substitution to make, and only §1 changes.**

- **Cookie sessions with `HttpOnly`, `Secure`, `SameSite=Strict`.** The
  strongest counter-proposal, because `HttpOnly` genuinely beats
  `sessionStorage` against XSS, which is the residual risk §2 accepts.
  Rejected because it introduces a second authentication mechanism beside
  ADR-0003's `Authorization` boundary — every route would need to accept
  either, and "either" is where authorization bugs live — and because an
  ambient credential brings the CSRF class with it, requiring `SameSite` plus
  an anti-forgery token on every mutating route. One boundary with a known,
  already-present XSS exposure was judged simpler to keep correct than two
  boundaries with a new class of failure between them.

- **Signed stateless session tokens (JWT).** Rejected for the reason ADR-0003
  already gave about bearer tokens, which applies with more force to sessions
  that must be revocable from a "sign out everywhere" button: "revocation
  would need a denylist table anyway; opaque hashed tokens are simpler and
  strictly easier to revoke."

- **Anonymous accounts with no contact address at all** (a credential printed
  once, like today's operator flow, but self-service). Maximally private, and
  rejected because it makes saved work disposable by construction: losing the
  printed credential loses the account with no path back, which is exactly
  what the gate's checklist forbids.

- **A profile, a directory, or a display name at registration.** Rejected as
  the overreach ADR-0003 named. An account that only its owner can observe is
  the smallest thing that unblocks saved analyses and evidence packets, and
  the social surface can be justified separately when a plan needs it.

## Consequences

**For `self-service-accounts`** (the implementing plan): registration and
sign-in routes, the credential table and the migration of existing rows into
it, session expiry, the `identity` rate-limit bucket plus the per-address and
global bounds, account export, account deletion with a freshness requirement,
and a mail-sending integration with bounce handling. Denial-path tests are the
point of the plan, not a garnish: unverified sign-in, expired link, reused
link, expired session, revoked session, cross-account access, the `202`
non-oracle on both a known and an unknown address, and proof that no
credential, code, or address reaches a log line, a cache key, or an error body.

**For `publishing-approval-path`:** it inherits `public_display_name`, the
snapshot-at-publish rule, and the unpublish-on-delete rule, and it owns
everything else about publishing.

**For the deployment:** one new required dependency (outbound email with a
credential), one configuration value that stops being optional
(`API_TRUSTED_PROXY_IPS`, per §4), and no new role, schema, engine, or
warehouse object. `BETA_RESET_REINGESTION.md` gains the re-run of
`002_app_api.sql`, which it already describes as the migration mechanic.

**For readers:** a visitor can hold an account, keep their own work, get it
all back in one request, and destroy it in one request. What the platform
knows about them is an email address and what they chose to create.

**Superseded:** ADR-0003's sentence "No self-service signup, no passwords, no
OAuth in this iteration" — self-service signup is now in scope, by the
mechanism ADR-0003 itself pointed at. Nothing else in ADR-0003 or ADR-0004 is
superseded; ownership scoping, optimistic concurrency, the `private, no-store`
answer, hard deletion, the read-only warehouse boundary, and the sharing
non-goal all survive this ADR intact.

**Corrected in passing:** ADR-0003 says "revocation is deleting the token
row", but `apps/api/auth.py` and `app_api.user_account` implement revocation
as stamping `revoked_at`, and `provision_app_api.py --revoke-token-label` says
so: "Stamps ``revoked_at``. The credential stops working immediately; the
account's configurations are left intact until the account is deleted."
Stamping is the better behaviour — it keeps an audit trail and prevents a
digest being reissued — and this ADR records it as the contract. Deleting the
row is what account deletion does, which is a different act with a different
consequence.
