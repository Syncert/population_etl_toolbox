---
id: catalog-total-single-source
branch: test/catalog-total-single-source
depends_on: []
parallel_safe: true
complexity: low
verify:
  - ./tests/run.ps1 unit
---

# The catalog's size is written once

## Plan status

- **Status:** Ready for review. Filed and delivered 2026-09-12, from a merge
  preview of the four plans finished that day.
- **Last updated:** 2026-09-12
- **Owner surface:** `tests/unit/shared/test_catalog_evidence.py`
- **Depends on:** nothing open.

## The defect, found by merging

Three of 2026-09-12's four plans each add one catalog row: ARC-006 (derived
grains), DB-029 (the migrations README), WEB-035 (the split style CSP). Each
correctly moved `TESTING_CONTRACT.md`'s total from `main`'s **291 to 292**,
and each correctly moved the hard-coded `291` in
`tests/unit/shared/test_catalog_evidence.py` to `292`.

Merging all four into a scratch branch produced **no conflict at all**. Three
identical edits to one line are one change, so git took it once: the register
then built **294** rows under a total that said **292**, and the number in the
test agreed with the wrong one.

The guard did fire — `test_behavioral_evidence_register_is_complete_and_explicit`
failed, because the row count it builds is real. But it failed by comparing
reality against a second restatement of the same fact, and it took a reader to
work out which of the two numbers was wrong and that *both* had to move. A
count written in two places is a count that can disagree with itself.

## Delivery

The test now reads the total from `TESTING_CONTRACT.md` rather than restating
it, so the document is the only place the size is written and the test is what
checks it against the register it builds. Its failure message says which way
the numbers disagree and what to do:

```
the register builds 292 rows and TESTING_CONTRACT.md declares 291;
adding a catalog row means updating the total beside it
```

A second check covers the other restatement in the same document: the prose
sentence describing what `python -m tests.support.catalog_evidence` renders
("the reviewable N-row register") must agree with the table's total, so a
reader who trusts the sentence and a reader who trusts the table are told the
same number. The `implemented of declared` pair in the total itself is checked
for agreement too.

## Acceptance

- [x] Adding a catalog row without updating the total fails with a message
      naming both numbers. Verified by inserting a row and leaving the total:
      `the register builds 292 rows and TESTING_CONTRACT.md declares 291`.
- [x] The prose count and the table total cannot disagree.
- [x] No number the register can derive is written in the test.

## Validation

| Check | Command | Result |
| --- | --- | --- |
| Focused | `python -m pytest tests/unit/shared/test_catalog_evidence.py` | 2 passed |
| Unit tier | `python -m pytest tests/unit` | 1341 passed |
| Gap | a catalog row added with the total left alone | fails, naming both numbers |

## What this does not fix

The total still has to be updated by hand when a row is added, and two
branches adding a row still both move it from N to N+1. This makes that
disagreement loud and single-valued rather than silent and doubled; it does
not make the count derive itself. Deriving it would mean generating the total
line into the document, which is a change to how the contract is authored and
belongs to whoever decides that.
