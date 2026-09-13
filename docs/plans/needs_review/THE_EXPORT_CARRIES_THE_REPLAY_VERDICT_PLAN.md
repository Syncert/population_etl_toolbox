---
id: the-export-carries-the-replay-verdict
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# The packet a reader is handed says which blocks can no longer be replayed

## Plan status

- **Status:** Needs review. Delivered 2026-09-13.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/evidencePackets.ts`

## Context

ADR-0004's whole distinction is that a packet leaves the building:

> a configuration is a live question you re-ask, a packet is a document you
> hand to someone else

`ComposedArticle` honours that on screen. It merges the API's per-block
verdicts and renders the ones that failed under a heading that says exactly
what happened — "The API reports these blocks can no longer be replayed as
composed" — with each block's reason.

Then the export drops it. `packetExport(packet)` takes the packet and nothing
else: it has no access to the validation, so its one state column,
`live_or_frozen`, reports `blockLiveStatus(envelope)` — "live", or "frozen to
release X". Those describe the block's *scope*. Whether the API can still
serve the block at all does not appear in the file.

So a packet whose measure was retired since it was composed exports as a CSV
whose row reads `frozen to release 2023-01-01`, and the reader who receives
the file — the reader the packet exists for — is never told that the screen
it came from said the block cannot be replayed. The module's own opening rule
is that this page "reports every block that lacks one instead of rendering it
as finished evidence"; the export renders it as finished evidence.

There is a second, smaller fault underneath, and it has to be fixed first or
the new column would lie. `mergeBlockStates` returns `state: "ok"` when there
is *no verdict at all* — a packet read from the browser draft rather than
from the account is never checked by the API — so "ok" means both "the API
checked this and it is valid" and "nobody checked". The existing unit node
pins that:

```js
const states = mergeBlockStates(packet, null);
expect(byId.evidence.state).toBe("ok");
```

Both on-screen consumers only filter for `"stale"`, so the conflation is
invisible there. Carried into a file it stops being invisible, and it
contradicts the rule the packet contract states in as many words:

> An absent verdict here means "not checked", never "valid".

## Acceptance criteria

1. `BlockReadState.state` distinguishes a checked-and-valid block from an
   unchecked one. Both existing consumers filter for `"stale"` and are
   unaffected.
2. `packetExport` carries the API's verdict per block: its state and its
   reason, in their own columns, beside the scope column rather than folded
   into it — `live`/`frozen` and `replayable`/`stale` are different facts.
3. A packet with no API verdict exports "not checked", never "replayable".
4. A prose block, which carries no analysis, carries no replay verdict
   either — the same as its empty `live_or_frozen` today.
5. The behaviour is a `TESTING_CONTRACT.md` catalog row (WEB-058).

## Non-goals

- Refusing the export. A reader may legitimately want the composition they
  have; the fix is that the file says what it is.
- Re-validating in the client. Only the API can see staleness, which is the
  reason `mergeBlockStates` exists; this carries its verdict, it does not
  second-guess it.

## Validation

**Failing first**, all four new nodes:

```
FAILED a stale block's verdict and reason travel in the file
FAILED no API verdict exports as not checked, never as replayable
FAILED an unchecked block is not the same state as a checked valid one
FAILED a prose block carries no replay verdict, as it carries no scope
```

**Two pre-existing nodes had to change, and both are evidence.**

The first is the conflation itself: `expect(byId.evidence.state).toBe("ok")`
for `mergeBlockStates(packet, null)` — a packet nobody checked, asserted to
be `ok`. It now asserts `unchecked`, and says why in the test.

The second was reading the state column by position:
`expect(analysisRow.at(-1)).toBe("live")`. `live_or_frozen` is no longer the
last column, and a positional assertion reads whichever column lands there —
so the node was rewritten to read by heading. It also now asserts that a
packet exported with no verdict says `not checked`, which is the case it was
already exercising without noticing.

**Both on-screen consumers were left showing exactly what they showed.**
`ComposedArticle` and `EvidencePacketBuilder` each computed
`mergeBlockStates(...).filter(state => state.state === "stale")` inline; each
now computes the states once and filters the same way, so the notices are
unchanged and the export gets the verdicts. The browser tier passing
unchanged is that claim: 77 passed, nothing a reader sees on either page
moved.

**What the file says now.** A stale block exports
`live_or_frozen: frozen to release 2022`, `replay_state: stale`,
`replay_reason: metric_code '…' is not a published metric`. The scope column
keeps its meaning; the verdict stands beside it rather than inside it.

**Green.**

| Tier | Command | Result |
|---|---|---|
| Frontend units | `npm --prefix apps/web run test:unit` | **304 passed** (was 300) |
| Frontend browser | `npm --prefix apps/web run test:browser` | 77 passed (2.0m) |
| Frontend lint / typecheck | `run lint`, `npx tsc --noEmit` | clean |
| Unit | `pytest tests/unit/shared` | 205 passed |

**Register.** 358 rows.

## Remaining work

- None.
