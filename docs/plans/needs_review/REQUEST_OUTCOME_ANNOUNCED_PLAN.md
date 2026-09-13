---
id: request-outcome-announced
branch: claude/iterate-plans-improvements-ir885c
depends_on: []
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:browser
  - npm --prefix apps/web run lint
---

# The outcome of a request the reader triggered is announced

## Plan status

- **Status:** Ready for review. Authored, claimed, and delivered 2026-09-13
  from an investigation of how the web app reports request state.
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/components/*.tsx`,
  `apps/web/app/catalog/page.js`, `apps/web/app/page.js`
- **Depends on:** nothing.
- **Next pickup:** none.

## Context

This application reports the outcome of every data request through one
shared vocabulary: a `StatusPill` in a `.status-row`, or the catalog's own
summary line. "Compatibility: incompatible — units differ", "loaded 3,144 of
18,864 records; the page bound cut the answer short", "1,204 matching
metrics", "The catalog could not be loaded: status 503". These are not
decoration; they are the facts that keep a reader from misreading the screen,
and this repository writes them with care.

None of them is announced. Eight `.status-row` sections and the catalog's
summary are plain `<section>` and `<div>` elements, so a status that changes
after a request changes silently. A reader using a screen reader clicks
**Compare**, the row flips from "checking" to "incompatible", and nothing is
said; types in the catalog search and the result count, the empty state, and
the error notice all change with no announcement.

The repository already holds itself to this standard elsewhere. The
accessibility suite asserts that selection state is announced
(`aria-live="polite"` on the selection panel), and the save toasts in the
explorer, the comparison workspace, and the profile product are `role="status"`.
So the pattern, the value, and the test are all established — the request
outcomes were simply missed, and they are the statuses a reader acts on most.

The catalog is the sharpest case because its search is live: there is no
submit button, the request is debounced on each keystroke, and the only
signal that anything happened is visual.

## Objective

Every screen has one polite live region carrying the outcome of the requests
that screen makes, and no screen becomes chatty in the process.

## Acceptance criteria

1. Each `.status-row` is a polite live region, so a status pill changing
   inside it is announced once, as a row.
2. The catalog's result state — the count and range, the loading state, the
   empty state, and the error — is one polite live region rather than four
   silent elements, and the home page's failure notice is announced.
3. `StatusPill` itself is **not** a live region. A catalog page renders one
   freshness pill per metric row; making the component live would turn a list
   render into dozens of announcements.
4. Regions are always present rather than conditionally inserted, so the
   first render sets the baseline and only later changes speak.
5. The behavior is asserted in the accessibility browser suite, across the
   core routes, and recorded as a `TESTING_CONTRACT.md` catalog row.

## Non-goals

- Changing any status wording, state vocabulary, or visual treatment.
- `role="alert"` (assertive) anywhere: a failed read is not an interruption
  that should cut off what the reader is listening to.
- A general audit of headings, contrast, or keyboard order; those have their
  own coverage and are not what this plan found.

## Evidence

### The gap, established first

Three tests were added to the accessibility browser suite and run before any
change. Two failed across the nine core routes: no `.status-row` carried
`role="status"`, and the catalog had no single result region. The third —
that `StatusPill` is *not* itself a live region — passed from the start and
is kept as the guard against the tempting wrong fix.

### What changed

- Eight `.status-row` sections — the explorer, the comparison workspace, the
  data-quality explorer, saved analyses, the composed article's two rows, the
  evidence-packet builder, and the profile product — carry `role="status"`.
  The pills inside are the content; a change within an existing region is
  what speaks.
- The catalog's four separate elements (summary, loading, error, empty) moved
  inside one always-present `role="status"` region. Its search is live —
  no submit button, debounced per keystroke — so this is the screen where
  silence cost the most.
- The home page's failure notice sits in an always-present region for the
  same reason: a region that appears only when it has something to say has
  no baseline to change from.
- `role="alert"` is used nowhere. A failed read is not an interruption that
  should cut off what the reader is already listening to.
- No wording, state vocabulary, or visual treatment changed.

### Commands

| Command | Result |
| --- | --- |
| `npm --prefix apps/web run test:browser` | 58 passed (Chromium), up from 55 |
| `npm --prefix apps/web run test:unit` | 227 passed |
| `npm --prefix apps/web run lint` | clean |
| `npm --prefix apps/web run typecheck` | clean |
| `npm --prefix apps/web run build` | succeeded |
| `npm --prefix apps/web run check:bundle` | every route within its declared budget |
| `npm --prefix apps/web run check:csp` | passed |
| `pytest tests/unit -q` | 1380 passed |
| `python -m tests.support.catalog_evidence` | 302-row register renders; WEB-037 is `FULL` |

### Not run

`make test-web-smoke` needs a live stack under Docker, which this environment
has no daemon for. It would add nothing here: these are static ARIA
attributes on the rendered markup, and the browser tier asserts them against
the real application on every core route.

## Remaining work

None.
