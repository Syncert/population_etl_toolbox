---
id: publishing-approval-path
branch: claude/publishing-approval-path
depends_on:
  - self-service-accounts
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit -q
  - python -m pytest -o addopts='' tests/integration/api -m "integration and not external"
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
  - ruff format --check . ; ruff check .
---

# A deliberate path from private to public

## Plan status

- **Status:** Unclaimed and still blocked on `self-service-accounts`, which is
  **built but not accepted** -- it sits in `in_progress/` awaiting one real
  sign-in against Google, which needs a credential no agent holds. Authored
  2026-09-15 from the repository assessment. There is nothing to publish until
  there is someone to publish it.
- **Last updated:** 2026-09-20
- **Current milestone:** not started.

### What is already there to inherit, as of 2026-09-20

ADR-0005 §3 names three things this plan inherits rather than builds. All three
exist now, so claiming this plan is a question of whether its dependency has
been *accepted*, not of whether the foundation is there:

- **`app_api.user_account.public_display_name`** -- nullable, absent until an
  account publishes, unique case-insensitively, and with whitespace normalised
  so a doubled space cannot impersonate a single one. Nothing from the provider
  is used as a default, and `display_label` is still an operator label that
  must never be rendered to a stranger. `PUT /api/v1/account/public-display-name`
  claims one; `apps/api/services/account_service.py` owns the bounds (API-152).
- **The snapshot-at-publish rule** is this plan's to implement, and the reason
  for it is already load-bearing: account deletion is a hard `DELETE` that
  cascades, so a published row joining live to the account would be a dangling
  reference the moment somebody leaves. Snapshot the name onto the published
  row and the row is simply destroyed with them.
- **Delete-on-account-deletion** -- the cascade is in the schema and exercised
  (API-152), and deletion now also survives a database restore through
  `app_api.account_deletion_log` and `scripts/apply_deletion_log.py` (API-155).
  A published-artifact table added by this plan gets that behaviour by
  declaring `ON DELETE CASCADE` on its owner column, and gets the
  backup-purge half for free.

One thing to read before starting, because it is the boundary this plan is
most likely to cross by accident: every identity route is deliberately outside
`CACHEABLE_ROUTERS` and answers `private, no-store`, swept rather than
spot-checked. A *published* artifact is the first thing on this platform that
must be the opposite -- public, cacheable, credential-free -- so it belongs in
`CACHEABLE_ROUTERS` beside the analytical reads rather than beside the account
routes it grew out of.

## Why

`AGENTS.md` names a "social hub" as what this repository is the foundation for,
and the first wave built everything a published artifact needs except the act
of publishing. An evidence packet is already a composition of analytical blocks
with a reproducibility envelope, validated per block at write and reported
rather than repaired on read (ADR-0004, `lib/evidencePackets.ts`). The
`/articles` surface already composes narrative around those blocks. What does
not exist is a reader other than the author.

`docs/reference/WEB_FIRST_WAVE_HANDOFF.md` states the design constraint
precisely, and this plan inherits it verbatim:

> A later publishing plan adds a *deliberate* path from private to public. It
> must be an explicit approval step with its own record, not a widening of any
> boundary above.

Those boundaries are specific and are the thing most easily lost: private
content never reaches a URL, private responses are never publicly cached, and
the bearer credential is never stored beside public data. A publishing feature
implemented as "make this row readable by everyone" breaks all three at once.

## Scope

**In scope**

1. **Publication as its own record, not a flag.** Publishing an evidence
   packet creates a distinct, immutable-at-a-version public artifact with its
   own identifier, its author's chosen public identity, the approval act, who
   performed it, and when. The private packet remains the private packet; a
   later edit to it does not silently change what a reader already has.
2. **A public read path with public-read semantics.** Published artifacts are
   served from the public, cacheable prefixes, with no owner scoping and no
   credential — and therefore nothing private in the response. The private
   `/evidence-packets` routes keep `private, no-store` and keep answering
   `404` across users.
3. **Unpublishing and takedown.** An author can withdraw a publication; an
   operator can take one down. Both are recorded. What a withdrawn artifact's
   URL answers is decided here and stated, because a reader holding the link
   is owed an answer rather than a broken page.
4. **The envelope survives publication.** Every block a reader sees carries
   what the author saw: source, measure, period, unit, uncertainty, scope and
   release, geography, caveats, and its live-versus-frozen basis, through
   `components/EvidenceEnvelope.tsx`. A published block whose measure was
   since retired reports that; it does not quietly re-resolve.
5. **Contract documentation**, including an ADR for the publication record and
   its retention, in the shape of ADR-0004.

**Out of scope**

- Comments, follows, reactions, feeds, notifications, or any social graph.
  Publishing is the first social primitive and is enough for one plan.
- Server-side rendering of user content beyond what a public artifact page
  requires, and any multi-tenant theming.
- Discovery ranking, trending, or recommendation of any kind.
- Moderation tooling beyond the operator takedown named above.

## Risks this plan must address explicitly

- **Cache poisoning of private content.** The public cache covers public
  prefixes. A published artifact that is later withdrawn must not remain
  readable from the cache past its takedown; state the invalidation and test
  it.
- **Identity leakage.** The author's public identity is whatever the identity
  ADR decided is public — never their credential, their account identifier, or
  a label an operator wrote for internal use.
- **Republication drift.** A published artifact references live warehouse
  publications by intent (ADR-0003's "a configuration is intent, not data").
  Decide and state what a reader sees when the underlying measure has moved:
  the frozen basis, the live one, or both with the difference visible.

## Acceptance criteria

- [ ] An author with an account can publish a packet, see it as an anonymous
      reader would, withdraw it, and see what a reader then gets.
- [ ] A published artifact is readable with no credential; the private packet
      routes remain owner-scoped, `private, no-store`, and `404` across users,
      proven by the existing isolation tests still passing unmodified.
- [ ] The approval act, its actor, and its time are recorded and inspectable;
      publication is never a side effect of a save.
- [ ] Takedown is effective in the cache as well as the database, and tested.
- [ ] Every analytical block in a published artifact carries its full envelope
      and its live/frozen basis, including a retired measure's state.
- [ ] No composite score, ranking, or causal claim is introduced anywhere in
      the publishing surface.

## Validation

The frontmatter commands, plus a recorded pass of the live-stack smoke tier
against a Compose stack — a public artifact is the first thing on this platform
an unauthenticated stranger loads, and it should be exercised as one.
