# Action: record the identity gate decision where the dispatcher can see it

> **Done — 2026-09-20.** Run on the dispatcher's own machine:
>
> ```bash
> python -m tools.plan_dispatcher approve --gate self-service-identity >     --by "Nick" --note "Identity contract accepted 2026-09-16: ..."
> ```
>
> `python -m tools.plan_dispatcher gates` now reports `self-service-identity`
> as `"status": "approved"`, `"decided_by": "Nick"`, decided at
> `2026-09-20T14:47:43+00:00`. Both checkboxes below are satisfied.
>
> The note also carries the value ADR-0005 left open — the provider is Google —
> but that record is the ADR's, not this file's, for the reason *One thing
> worth deciding later* gives below: the dispatcher's state file is gitignored
> and machine-local, so nothing that has to survive a fresh clone may live only
> there. ADR-0005 §1, *The provider, named*, is the durable copy.
>
> The gate itself is **not** retired; see the last section.

**This is an action, not a test.** You already made the decision; this writes
it somewhere the dispatcher reads.

## What this is

You accepted the self-service identity contract on 2026-09-16.
[`docs/decisions/0005-self-service-accounts.md`](../../../decisions/0005-self-service-accounts.md)
is `Accepted` on `main`, which is the durable record and the one that
survives a fresh clone.

What has *not* happened is the dispatcher-side record. Until it does,
`plan_dispatcher` still treats `self-service-identity` as an open gate and
will not dispatch `self-service-accounts` or `publishing-approval-path`.

## Why an agent could not do it

`tools/plan_dispatcher` writes gate decisions to
`.claude/plan-runner-state.json`, and `.gitignore` excludes that file
(line 471). It is machine-local by design. The container this work was done
in had no dispatcher state at all — `plan_dispatcher gates` answered *"No
dispatcher run state … Start a run with `init-run`"* — so approving there
would have written a decision into a file that is never committed and dies
when the container is reclaimed.

The command **was** verified against a throwaway state file, so it will not
surprise you: the gate is open for review, and it approves cleanly.

## You need

The machine your dispatcher runs on, with its
`.claude/plan-runner-state.json` — **not** a fresh clone.

**Time:** about 2 minutes.
**Touches:** one gitignored local state file. Nothing is committed.

## Steps

**1. Run the approval:**

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate self-service-identity `
    -By "Nick" -Note "Identity contract accepted: OIDC, memory + HttpOnly refresh hybrid, deletion covers backups."
```

The Python equivalent, if you would rather:

```bash
python -m tools.plan_dispatcher approve --gate self-service-identity \
    --by "Nick" --note "Identity contract accepted."
```

**2. Confirm it took:**

```bash
python -m tools.plan_dispatcher gates
```

## What good looks like

Step 1 prints a record like:

```json
{
  "id": "self-service-identity",
  "title": "Gate: the self-service identity contract",
  "status": "approved",
  "decided_by": "Nick",
  "decided_at": "..."
}
```

and step 2 shows `self-service-identity` as `approved`.

- [ ] The gate reads `approved`
- [ ] `decided_by` is your name, not an agent's

## If it fails

**"No dispatcher run state"** — the dispatcher has no run open on this
machine. Start one (`init-run`) or run the approval through
`Invoke-ClaudePlans.ps1`, which manages the run for you.

**"Gate is not open for review yet"** — the gate's dependency
`self-service-identity-adr` is not visible as complete. It is in
`docs/plans/completed/` on `main`; check the clone you are running from is
up to date.

## One thing worth deciding later

A gate decision that lives only in a gitignored, machine-local file cannot be
audited from the repository, cannot survive a fresh clone, and cannot be seen
by anyone reading history — for a checkpoint whose entire purpose is
recording that a human decided something irreversible.

Here it is covered, because ADR-0005's `Accepted` status is committed. A
future gate that guards work with no ADR behind it would have no durable
record at all. That is a design question rather than a bug, and it
contradicts the gate file's own "recorded through the dispatcher, not in this
file" instruction, so it needs your decision rather than a patch.

## Retiring the gate — not yet

Do not retire this gate now. [`docs/plans/README.md`](../../README.md) retires a
gate only once the work it guards is accepted, and retiring means stripping
the `kind: gate` frontmatter **and** removing its id from every dependent in
the same change. An archived gate that keeps its frontmatter holds its
dependents forever; a deleted one whose id is still named in a `depends_on`
makes `validate_graph` reject the whole inventory.
