---
id: no-prefix-as-a-whole-list
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A bounded read is never handed back as the whole list

## Plan status

- **Status:** Accepted 2026-09-14 (Needs review. Delivered 2026-09-13.)
- **Last updated:** 2026-09-14
- **Owner surface:** `apps/web/lib/api/client.ts`

## Context

`fetchCollectionPages` was built to be honest about its bound, and says so:

> Deterministic limit/offset paging over `{items, total}` collection
> responses. Bounded so a contract regression cannot loop forever, and
> **honest about it: a caller that hits the bound is told the answer is a
> prefix rather than handed a truncated list as if it were whole.**

Then the convenience wrapper beside it throws that away:

```ts
export async function fetchAllPages<T>(resource, options = {}): Promise<T[]> {
  const { items } = await fetchCollectionPages<T>(resource, options);
  return items;
}
```

`complete` is computed and dropped. Every caller in the application uses the
wrapper — the explorer's measure list and its state and county pickers, the
profile product's place pickers, the data-quality explorer's metric list —
so each one can be handed a prefix with no way to know. The data-quality
explorer then states it as a fact: `${items.length} metrics published for
${selectedSource}`.

The bound is 50 pages of 1,000, so nothing reachable today overflows it:
3,143 counties and a few thousand metrics per source. This is the signal
being discarded, not a present miscount — and it is discarded in the one
helper whose sibling exists to produce it.

## Acceptance criteria

1. `fetchAllPages` never returns a prefix. A read cut short by the page bound
   raises, naming the resource, what it got, and what the API reported.
2. Every existing caller reports it, without a change: each already has a
   failure path that renders `apiErrorMessage`.
3. `fetchCollectionPages` is unchanged — a caller that wants the prefix and
   the flag still has them.
4. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-056).

## Non-goals

- Raising the bound. 50,000 records is already far past any list a person
  picks from; a list that large is a different interaction, not a bigger
  fetch.
- Rewriting the callers to render a partial list with a warning. That is a
  design decision per screen; refusing to hand back a prefix is the
  correctness fix underneath it, and it is what makes the decision visible.

## Validation

**Failing first**, and then a second failure worth recording.

The new nodes fail against the old wrapper — a cut-short read resolved with
its prefix instead of raising. Then, with the change in, a *pre-existing*
test failed:

```
FAIL  api-client.test.js > stops paging on an empty page and on the page bound
IncompleteCollectionError: /catalog/metrics answered 2 records within the
page bound; that is a prefix, not the whole list
```

That test fed a collection publishing **no total** and asserted the two rows
it managed to read. `fetchCollectionPages` already called that `complete:
false` — with no total the client cannot know whether more exist — so the
wrapper refusing is the same judgement, and the test now asserts the bound
(two requests, unchanged) and the refusal. It is the clearest evidence of
what was being discarded: the old behaviour was to hand back rows the helper
itself did not consider a complete answer.

**The message names what a reader needs**:
`/catalog/geographies answered 4 of 5 records within the page bound; that is
a prefix, not the whole list`. `apiErrorMessage` renders an `Error`'s
message, and every caller already has a failure path, so no caller changed.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | 295 passed (was 292) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 77 passed |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit` | 1467 passed |

The browser tier passing unchanged is the second half of the claim: every
list the application builds fits inside the bound today, so nothing a user
sees changes — what changed is that an overflow would now say so.

**Register.** 353 rows.

## Remaining work

- None.
