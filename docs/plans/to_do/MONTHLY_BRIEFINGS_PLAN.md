---
id: monthly-briefings
depends_on:
  - publishing-approval-path
  - place-pages
parallel_safe: false
complexity: high
verify:
  - python -m pytest tests/unit/api -q
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run test:browser
---

# Monthly briefings

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet. Blocked on `publishing-approval-path`, which supplies
the private-to-published approval the briefing is the first consumer of.

## Why

The almanac is evergreen; the briefing is the reason to come back. Each
month: which releases landed (LAUS, CPI, a PEP vintage, an ACS release, a
PLACES release), what moved, and which places it touched, written from
evidence blocks so every claim is reproducible. It is also the monthly
episode: the blocks are the segments, published the same day as the video.

## What exists

- The evidence packet builder (`/builder`) composes blocks with a
  reproducibility envelope; the article reader (`/articles`) presents a
  packet exactly as composed with the API's per-block verdict.
- `GET /api/v1/catalog/freshness` reports per-source publication state.
- `publishing-approval-path` (to do) will define how a private packet
  becomes public.

## Deliverables

1. **Briefing kind.** A published evidence packet flagged as a briefing with
   a `yyyy-mm` period, additive to the packet schema; the approval path's
   rules apply unchanged. One briefing per month is enforced at publish.
2. **Released-this-month block.** A block type generated from the freshness
   resource listing the source publications whose release date falls in the
   month, with their periods; it is data, not prose, and re-renders from the
   resource.
3. **Places-touched block.** A block type listing the geographies the
   briefing's evidence blocks read, linking to their place pages.
4. **Routes.** `/briefing` lists published briefings newest first;
   `/briefing/<yyyy-mm>` renders one through the article reader with its
   envelopes and verdicts; the latest briefing feeds the home page strip and
   the home map's painted measure (`find-your-place-home`).
5. **Video link.** An optional external link to the episode; absent, nothing
   renders.

## Acceptance criteria

- A packet flagged as a briefing for a month publishes through the approval
  path and renders at `/briefing/<yyyy-mm>`; a second briefing for the same
  month is refused with a stable error.
- The released-this-month block lists only publications the freshness
  resource reports for that month and re-renders from it; a unit test
  covers a month with no releases (the block says so, not an empty list).
- The places-touched block names every geography read by the briefing's
  evidence blocks and links each to its place page.
- Every analytical block on the briefing carries its envelope and the API's
  verdict, as `/articles` already guarantees; a block without an envelope is
  named, not rendered as evidence.
- `/briefing` lists only published briefings; drafts are never served
  publicly, and the browser scenario asserts a draft is absent.
- OpenAPI snapshot, consumer guide, API unit tests, Ruff, and the web tiers
  pass.

## Open items to resolve during implementation

- Whether the briefing flag is a packet attribute or a separate published
  collection; follow what `publishing-approval-path` lands.
- Attribution of the published author on the public page, within the
  privacy boundary ADR-0005 sets.

## Checkpoint

Next pickup: after `publishing-approval-path` reaches `needs_review/`, read
its resource shape, then write the failing API test for a briefing packet of
one month.
