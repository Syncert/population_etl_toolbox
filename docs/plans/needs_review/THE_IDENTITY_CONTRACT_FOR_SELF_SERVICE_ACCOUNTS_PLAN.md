---
id: self-service-identity-adr
branch: claude/self-service-identity-adr
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m tools.plan_dispatcher inventory
---

# The identity contract for self-service accounts

## Plan status

- **Status:** Ready for review. The ADR is written and every acceptance
  criterion has inspectable evidence. The decision itself is the reviewer's,
  and `gates/SELF_SERVICE_IDENTITY_GATE.md` is where it gets recorded.
- **Last updated:** 2026-09-15
- **Current milestone:** complete.
- **Dependencies:** none declared; none required.
- **Next pickup:** none.

## Why

Every write path the platform has runs on an operator-provisioned bearer token
(`apps/api/auth.py`), which a reader pastes into the browser and which lives in
`sessionStorage` for one tab (`apps/web/lib/apiToken.ts`). That was a decision,
not an oversight: ADR-0003 chose it deliberately because "the consumer is the
project's own web application and its operators", and recorded that "every
deferred alternative (sessions, OIDC) can be added behind the same
`Authorization` boundary later without moving stored data".

Later is now the blocking constraint. `AGENTS.md` states the repository is the
foundation for "a public-data analytics website and social hub"; a visitor
who cannot obtain a credential cannot save an analysis, cannot own an evidence
packet, and cannot be the subject of any social feature. Both the saved-analysis
and evidence-packet contracts (ADR-0003, ADR-0004) are already written in terms
of an `owner_user_id` that only an operator can mint.

ADR-0003 also set the precedent for how this is decided: API-007 was forbidden
from starting until an authentication, authorization, ownership, privacy,
retention, and deletion contract was approved by a human. The same question is
open again, with a larger blast radius — anonymous accounts, credential
recovery, and abuse are all new — so it gets the same treatment, and this plan
delivers the decision document rather than the implementation.

## Scope

**In scope:** one ADR, `docs/decisions/0005-self-service-accounts.md`, written
to the shape of ADR-0003 (Context, Decision, Rejected alternatives,
Consequences) and answering, at minimum:

1. **Registration and credential.** What a visitor presents to create an
   account, and what the platform stores. Whether that is email plus password,
   a third-party OIDC provider, or an email-link credential with no password
   at all — and what the recovery path is for each, since a platform that
   cannot recover an account silently makes saved work disposable.
2. **Session versus token.** Whether browser sessions become a second
   authentication mechanism beside bearer tokens or a way of issuing them.
   ADR-0003's boundary — one `Authorization` header, a hashed opaque
   credential, revocation by stamping a row — is the thing to preserve or to
   consciously replace. If sessions use cookies, say what stops a cross-site
   request from spending one.
3. **What an account is allowed to be.** ADR-0003 explicitly refused to
   "overreach into a social account system it cannot yet justify". This ADR
   decides how far that now extends: a display identity that other users can
   see is a different privacy object from an operator label, and publishing
   attaches a name to a public artifact.
4. **Abuse and cost.** Registration is the first unauthenticated write the
   platform will accept. What bounds it, how `apps/api/ratelimit.py` applies,
   and what an operator can do about an account that abuses it.
5. **Privacy, retention, deletion, export.** ADR-0003's answers — hard delete,
   effective immediately, `GET` is its own export, no derived retention —
   extended to account-level data, including what deleting an account does to
   content someone else may already be reading.
6. **Migration.** What happens to the operator-provisioned tokens and accounts
   that exist when this ships. They are not to be invalidated silently.

**Out of scope:** any implementation. No schema, no route, no UI, no test. A
worker that starts writing code under this plan has taken the decision it was
supposed to put to a human.

## Acceptance criteria

- [x] `docs/decisions/0005-self-service-accounts.md` exists, in `Proposed`
      status, answering all six questions above with the alternative it
      rejects and why.
- [x] Every constraint it inherits is quoted from its source rather than
      paraphrased: the read-only warehouse role, private responses outside the
      public cache, telemetry that logs no headers or bodies, and ADR-0003's
      ownership-scoped-in-SQL rule.
- [x] ADR-0003 and ADR-0004 are cross-referenced, and anything this ADR
      supersedes in them is named explicitly.
- [x] `docs/plans/gates/SELF_SERVICE_IDENTITY_GATE.md` is satisfied by the
      document — that is, a reviewer can answer the gate's checklist from the
      ADR alone.
- [x] No implementation file is touched by this plan's commit.

## What the ADR decides

546 lines at `docs/decisions/0005-self-service-accounts.md`, in `Proposed`
status. The six answers, in the order Scope asks them:

1. **Credential.** An emailed single-use sign-in link; no password. The
   database holds a case-folded unique email and `sha256(link_token)` with a
   15-minute expiry, and never a presentable secret. **Recovery is the sign-in
   path** — there is no second flow to build weaker than the front door and no
   second secret whose loss strands saved work.
2. **Session versus token.** ADR-0003's single `Authorization: Bearer`
   boundary is preserved exactly; a session *issues* that credential rather
   than being a second mechanism. No cookie authenticates anything, so the
   gate's cross-site question is answered by construction: there is no ambient
   credential for a cross-site request to spend.
3. **What an account may be.** Private by default — no directory, no profile,
   no way for one account to learn another exists. A public display name is
   nullable and chosen at first publish, kept separate from the existing
   operator `display_label`, and snapshotted onto the published row.
4. **Abuse and cost.** A third `identity` rate-limit bucket beside `catalog`
   and `analysis`, plus a per-address bound (the one that stops the service
   being used to mail-bomb a stranger, which a per-IP bucket cannot do) and a
   global per-hour ceiling on account creation.
5. **Privacy, retention, deletion, export.** ADR-0003's answers extended to
   the account: hard delete, immediate, cascading; an account-level export,
   because per-resource `GET` is not one; and deletion unpublishes and deletes
   published artifacts, with the limit of that promise stated — the platform
   can stop serving a copy, not recall one.
6. **Migration.** Existing operator tokens keep working unchanged, with no
   expiry and no forced migration; the digest is copied, not regenerated, so
   the tokens in circulation stay the same tokens.

## The decision the reviewer should weigh first

**Email-link versus third-party OIDC.** This was the closest call, and the ADR
recommends the email link while recording OIDC as the substitution to make if
the reviewer prefers it. The trade is one dependency for another: email-link
makes outbound mail a required deployment dependency the platform does not
have today (provider, credential, SPF/DKIM, bounce handling), while OIDC
removes that and instead makes signing in to a public-data site conditional on
holding an account with a particular company, and tells that company who reads
this site.

The ADR is structured so that flipping this changes §1 only — §2 is
deliberately written so that any future credential source is one more way to
mint a session, not a second authentication mechanism, which is ADR-0003's own
"can be added behind the same `Authorization` boundary later" applied forward.

Two other choices a reviewer may want to overturn, both argued in *Rejected
alternatives* rather than assumed:

- **`sessionStorage` over `HttpOnly` cookies.** Cookies genuinely beat
  `sessionStorage` against XSS. The ADR keeps one boundary with a known
  exposure over two boundaries with CSRF between them, and says so plainly
  instead of claiming the choice is free.
- **Deleting published artifacts on account deletion**, rather than keeping
  them with the author redacted. The ADR chooses to honour deletion without an
  asterisk and accepts that a reader's link stops resolving.

## Evidence

| Check | Result |
|---|---|
| All 16 block quotes verified verbatim against their source files | 16/16 |
| Inline `AGENTS.md` quote verified verbatim | OK |
| Gate checklist, all seven lines answerable from the ADR alone | 7/7 |
| `python -m tools.plan_dispatcher inventory` (declared verification) | graph resolves |
| `python -m pytest tests/unit -q` | 1753 passed |
| `ruff format --check .` / `ruff check .` | 473 formatted; all checks passed |
| `git status` — implementation files touched | none; the ADR and this plan's move only |

The quote check is mechanical rather than eyeballed: each `>` block was
normalized and matched as a substring of its source file, so a constraint that
had drifted into a paraphrase would have failed rather than read plausibly.

## Findings recorded in the ADR

- **ADR-0003 and the implementation disagree about revocation.** ADR-0003 says
  "revocation is deleting the token row"; `apps/api/auth.py`,
  `app_api.user_account`, and `provision_app_api.py --revoke-token-label` all
  stamp `revoked_at` instead. Stamping is the better behaviour — it keeps an
  audit trail and stops a digest being reissued — so ADR-0005 records the
  implemented behaviour as the contract rather than "correcting" working code
  to match a sentence.
- **`API_TRUSTED_PROXY_IPS` stops being optional.** For catalog reads an
  unconfigured trusted-proxy list is a tuning error; for registration it is
  the difference between a bound and no bound, because the per-client budget
  becomes one budget for the whole internet.
- **`display_label` must not be reused as a public name.** It is `NOT NULL`
  today and holds an operator label written on the assumption nobody outside
  the deployment would read it.

## Not done, deliberately

No implementation, per the plan's own out-of-scope line: no schema migration,
no route, no UI, no test. The DDL in the ADR is illustrative, in the shape
ADR-0004 already uses, and touches no file under `sql/`, `apps/`, or `tests/`.

The gate is not moved or approved. `gates/SELF_SERVICE_IDENTITY_GATE.md`
records its decision through the dispatcher, by a person, and retiring it
happens when the accounts work is accepted — not here.

## Validation

```bash
python -m tools.plan_dispatcher inventory   # the graph still resolves
```

The real validation is human: this plan exists to produce a decision, and the
gate that follows it is where that decision is recorded.
