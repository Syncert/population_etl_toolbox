---
id: self-service-identity
kind: gate
depends_on:
  - self-service-identity-adr
---

# Gate: the self-service identity contract

## What this gate guards

`self-service-accounts` and, behind it, `publishing-approval-path`. Neither may
start until a person has accepted the identity contract drafted by
`self-service-identity-adr`.

## Why it is a gate rather than a plan dependency

ADR-0003 forbade API-007 from starting before an authentication,
authorization, ownership, privacy, retention, and deletion contract was
approved by a human, and that precedent is the reason this checkpoint exists in
the same shape. The questions a self-service account raises — what credential a
stranger presents, what other readers can see about them, what deleting an
account does to something another reader has already opened — are not
answerable by any one plan's test suite, which is what
`docs/reference/PLAN_DISPATCHER.md` says a gate is for.

Getting identity wrong is also the least reversible mistake available here.
Stored credentials, an account's public identity, and the rows that reference
`owner_user_id` are all expensive to take back once real users exist.

## Review checklist

Approve only when every line is true of
`docs/decisions/0005-self-service-accounts.md`:

- [ ] The credential a visitor presents, what the database stores, and the
      recovery path are all named — and the recovery path does not make saved
      work disposable.
- [ ] Sessions, if chosen, are reconciled with ADR-0003's single
      `Authorization` boundary rather than added beside it without comment,
      and a cookie-based session states what stops a cross-site request from
      spending one.
- [ ] The account's public identity is decided deliberately, including what a
      reader of a published artifact will see.
- [ ] Registration's abuse and cost bounds are stated, with the existing rate
      limiter's role in them.
- [ ] Retention, export, and deletion extend ADR-0003's answers to the account
      itself, including the effect of deletion on content others can read.
- [ ] Existing operator-provisioned tokens have a stated migration and are not
      invalidated silently.
- [ ] The read-only warehouse boundary, the private-response cache rule, and
      the no-headers/no-bodies telemetry rule are all preserved.

## Decision

Recorded through the dispatcher, not in this file:

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate self-service-identity `
    -By "your name" -Note "..."
```

Retire the gate once the accounts work is accepted, per *Retiring a gate* in
`docs/reference/PLAN_DISPATCHER.md`: move it to `completed/`, strip this
frontmatter, and remove its id from every dependent in the same change.
