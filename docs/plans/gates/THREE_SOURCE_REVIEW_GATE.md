---
id: three-source-review
kind: gate
depends_on:
  - cdc-illness
  - fbi-crime
  - usda-crop
---

# Three-source warehouse review gate

This gate stops the dispatcher once the CDC, FBI Crime, and USDA NASS Crop
pipelines have all been implemented, verified, and integrated. Nothing that
consumes those three sources may start until a human approves it.

## Why this gate exists

The three source plans are implemented in parallel by independent workers. Each
one is verified against its own acceptance criteria, but nothing in the
automated run checks the questions that only matter once all three exist
together:

- whether the three sources agree on shared geography, time, and revision
  semantics;
- whether their gold products can be compared without the API compensating for
  a source-specific decision;
- whether three separately-reasoned adapters have quietly diverged from
  `docs/reference/ADDING_A_DATA_SOURCE.md`; and
- whether the combined warehouse is worth building the API platform on.

A per-plan test suite cannot answer those. A human looking at all three
diffs together can, and this is the cheapest point to answer them — before the
warehouse-quality, end-to-end, and API plans build on the result.

## What a reviewer must confirm

- [ ] Each source preserves provider grain, identity, units, suppression, and
      revision semantics without inventing a value the provider did not publish.
- [ ] The three sources resolve geography through the shared versioned
      dimensions rather than source-local mappings.
- [ ] Capture, replay, and quarantine behave consistently across all three.
- [ ] Gold products and glossary entries follow one publication contract.
- [ ] No source-specific compensation has leaked into shared code.
- [ ] The plans' own acceptance criteria are met rather than reinterpreted.
- [ ] The integration branch is coherent: no conflicting migrations, duplicated
      shared objects, or contradictory DDL ordering across the three merges.

## Approving or rejecting

From the repository root, once the run reports the gate is awaiting review:

```powershell
./tools/Invoke-ClaudePlans.ps1 -Action approve -Gate three-source-review `
    -By "your name" -Note "reviewed all three source diffs"

./tools/Invoke-ClaudePlans.ps1 -Action reject -Gate three-source-review `
    -By "your name" -Note "CDC and PEP disagree on county vintage handling"
```

Approving lets the dependent plans dispatch on the next tick. Rejecting blocks
every dependent plan and ends the run, so the problem is fixed deliberately
rather than built upon. `-Action reopen` clears a recorded decision if it was
made in error.

The decision, who made it, when, and the note are recorded in the run-state
file, so a later reader can see the checkpoint was actually cleared by a
person.
