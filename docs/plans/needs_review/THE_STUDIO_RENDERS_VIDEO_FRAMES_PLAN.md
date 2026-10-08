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

Ready for review, 2026-10-06, on branch `feat/studio-frames`, stacked on
`feat/place-pages` (it uses the chapter contract), so it merges after that one.

### Implementation evidence

- **Route:** `/studio` (`app/studio/page.js`, `components/StudioPage.tsx`):
  with no credential it renders the sign-in control and no frame; with a
  session or operator token it renders the picker (state, county, chapter,
  card, format, theme) and a default frame (Dane County, the first People
  headline). `/studio` is in `PRIVATE_ROUTES`, so robots disallow it and the
  sitemap omits it.
- **Frame renderer:** `lib/studioFrame.ts` `renderFrameSvg(spec)` draws the
  frame as SVG at the format's exact size with video type, the fixed County,
  State and Nation colours, bars from zero, and a footer (measure and code,
  period and source, each level's uncertainty, the chapter caveat, the site
  name) that the one drawing function always draws: it takes no option, and
  a spec carrying `footer: false` still draws it. Values are the page's own
  newest-value responses; a parent whose newest period differs is shown as
  "not published for <period>".
- **Exports:** PNG by rasterising that SVG onto a canvas at the declared
  size (no screenshot service; `img-src blob:` already admits it); CSV
  through `observationExport`; the frame record as JSON.
- **Frame record and history:** reuses the saved-analysis resource (the
  schema fits without change): a `workbench` document with one observations
  series per level (`newest_per_geography` only where the source declares
  that reduction) and a `bar` presentation, and in `visualization` the exact
  request URLs, release pins, format, theme, place, chapter and card. The API
  accepts it (`test_a_studio_frame_record_is_a_valid_workbench`). Saved
  records are listed (named `Frame · ...`) and reopen at
  `/studio?record=<id>`, which replays the stored requests exactly. The
  token travels only in the Authorization header, and the resource answers
  `private, no-store`.
- **Script notes:** read-only: the reviewed definition where one exists,
  otherwise the harvested label marked `not reviewed`, the chapter and card
  caveats, the period sentence, and the requests. `docs/semantics/` holds no
  reviewed definition yet, so every frame reads `not reviewed` today; the
  registry (`REVIEWED_DEFINITIONS`) is the one place a reviewed definition
  is added, and the unit tier covers both states. The explainer link waits
  for `explainer-pages`.

### Decisions on the open items

- PNG through canvas from a blob URL of the frame's own SVG: permitted by
  the current CSP (`img-src 'self' data: blob:`), and `check:csp` passes.
- Frame records reuse the saved-analysis resource with the `workbench` kind;
  no new resource or schema change was needed.

### Validation (local, Windows, 2026-10-06)

- `npm --prefix apps/web run test:unit`: 51 files, 746 tests
  (`studio-frame.test.js`, 9).
- `lint`, `typecheck`, `build`, `check:csp`, `check:bundle` (budget
  declared for `/studio`): passed.
- `npx playwright test`: passed, including `studio.spec.js` (no credential
  shows sign-in and no frame; PNG exports measured 1920x1080, 1080x1920 and
  1080x1080 from the downloaded bytes; save and reopen replays the stored
  requests; the token never appears in a URL).
- `python -m pytest tests/unit/api/test_saved_analysis.py`: passed.
- Frames were also rendered and inspected as images at 16:9 (light) and
  9:16 (dark).

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

Implementation complete; awaiting human review.
