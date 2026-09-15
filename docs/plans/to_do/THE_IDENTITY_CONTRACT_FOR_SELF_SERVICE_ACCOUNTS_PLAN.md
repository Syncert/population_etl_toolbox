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

- **Status:** Unclaimed. Authored 2026-09-15 from the repository assessment;
  no implementation has started.
- **Last updated:** 2026-09-15
- **Current milestone:** not started.

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

- [ ] `docs/decisions/0005-self-service-accounts.md` exists, in `Proposed`
      status, answering all six questions above with the alternative it
      rejects and why.
- [ ] Every constraint it inherits is quoted from its source rather than
      paraphrased: the read-only warehouse role, private responses outside the
      public cache, telemetry that logs no headers or bodies, and ADR-0003's
      ownership-scoped-in-SQL rule.
- [ ] ADR-0003 and ADR-0004 are cross-referenced, and anything this ADR
      supersedes in them is named explicitly.
- [ ] `docs/plans/gates/SELF_SERVICE_IDENTITY_GATE.md` is satisfied by the
      document — that is, a reviewer can answer the gate's checklist from the
      ADR alone.
- [ ] No implementation file is touched by this plan's commit.

## Validation

```bash
python -m tools.plan_dispatcher inventory   # the graph still resolves
```

The real validation is human: this plan exists to produce a decision, and the
gate that follows it is where that decision is recorded.
