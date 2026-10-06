---
id: studio-video-frames
depends_on:
  - place-pages
parallel_safe: true
complexity: high
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
  - npm --prefix apps/web run typecheck
  - npm --prefix apps/web run build
  - npm --prefix apps/web run check:csp
  - npm --prefix apps/web run check:bundle
  - npm --prefix apps/web run test:browser
---

# The studio renders video frames

## Status

To do. Drafted 2026-10-06 from
[`docs/product/PLACE_ALMANAC_WEBSITE_PLAN.md`](../../product/PLACE_ALMANAC_WEBSITE_PLAN.md).
No implementation yet.

## Why

The almanac is also a channel. Preparing a video should mean choosing a page,
not building a deck. The studio is an operator-only route that renders any
place-page card or chart as a 16:9 or 9:16 frame with large type, the three
level colours, and the period, source, and caveat line baked into the image,
and assembles script notes from the reviewed definition and caveats so the
voice-over says what the documentation says and nothing more. Every frame
exported from the studio keeps its provenance even when clipped out of
context, which is what protects the channel's credibility.

## What exists

- Chart components (`BarChart`, `LineChart`, `TimeSeriesChart`,
  `ChoroplethMap`) and the evidence envelope (`EvidenceEnvelope.tsx`) render
  every value with its request context.
- Chart saving, CSV export, and history export exist in the workbench and
  the use-case evidence panel (`lib/observationExport.ts`,
  `lib/savedCharts.ts`).
- Account-owned storage behind a bearer credential (ADR-0003, ADR-0004)
  covers saved analyses and evidence packets.

## Deliverables

1. **Route.** `/studio`, available only with a bearer credential (operator
   token or signed-in session); unauthenticated visitors see the sign-in
   control and no frame. The route is excluded from the public sitemap.
2. **Picker.** Place, chapter, and card or chart, driven by the chapter
   contract module from `place-pages`; frame format (16:9 at 1920x1080,
   9:16 at 1080x1920, 1:1 at 1080x1080); light or dark theme.
3. **Frame renderer.** The chosen component in a frame layout with a type
   scale for video, the fixed County, State, and Nation colours, and a
   footer baked into the frame: measure, period, source, uncertainty, site
   name. The footer cannot be hidden. Values are read from the same API
   responses the page uses; the frame never recomputes.
4. **Exports.** PNG of the frame (canvas or SVG serialization of the same
   DOM, not a screenshot service), the CSV of the frame's observations via
   the existing export path, and a JSON "frame record" with the exact
   requests, period, and release pin so the frame can be re-rendered when a
   source revises.
5. **Script notes.** A panel that assembles, read-only: the reviewed
   definition text (from `docs/semantics/` where one exists, else the
   harvested label with the explicit `not reviewed` state), the source
   caveats, the period sentence, and the explainer link. No free-text
   authoring in the studio; it reads, it does not write.
6. **Frame history.** Exported frame records are saved to the account like
   saved analyses, listed with their requests, and reopenable.

## Acceptance criteria

- `/studio` without a credential renders sign-in and no frame; with the
  operator token it renders the picker and a default frame.
- A rendered frame contains the footer with measure, period, source, and
  uncertainty; a unit test asserts the footer is present in every format
  and cannot be toggled off.
- PNG export at each format produces an image of the declared pixel size;
  the browser scenario asserts dimensions from the exported blob.
- The frame record contains the exact observation requests and release pin;
  reopening a record re-renders the same frame from those requests.
- Script notes show the reviewed definition where one exists and the
  explicit `not reviewed` state otherwise; the unit test covers both.
- Frame records are stored and served `private, no-store` with the bearer
  token only in the Authorization header, never in a URL.
- `/studio` is absent from the sitemap; `check:csp` passes (no new inline
  styles beyond the data-driven attributes the CSP already admits); web
  tiers pass.

## Open items to resolve during implementation

- PNG generation under the CSP: canvas drawing from an inline SVG data URL
  is permitted by the current `img-src blob:` and `data:` rules; verify
  before choosing the path.
- Whether frame records reuse the saved-analysis resource with a kind
  discriminator or need their own resource; prefer reuse if the schema fits
  without weakening it.

## Checkpoint

Next pickup: read `lib/savedCharts.ts` and the evidence envelope, then
write the failing unit test for the mandatory footer in the frame layout.
