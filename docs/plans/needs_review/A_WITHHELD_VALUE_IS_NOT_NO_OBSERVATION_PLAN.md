---
id: a-withheld-value-is-not-no-observation
branch: claude/iterate-plans-improvements-ir885c
depends_on: [a-gap-in-a-history-is-named]
parallel_safe: true
complexity: low
verify:
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run test:browser
---

# A withheld value is not the same as no observation

## Plan status

- **Status:** Needs review. Investigated, authored and implemented
  2026-09-13. **Found by asking the API-127 question of the map instead of
  the chart.**
- **Last updated:** 2026-09-13
- **Owner surface:** `apps/web/lib/explorerViewModel.ts`,
  `apps/web/lib/comparison.ts`

## Context

API-127 split the sources into two shapes; WEB-077 taught the chart to read
the second. The map had the opposite problem, on the sources that *do*
publish a value state.

`buildChoroplethModel` skipped any row without a published number:

```ts
const numericValue = publishedNumber(item.value);
if (!joinValue || numericValue === null) {
  continue;
}
```

So a CDC county whose estimate was **suppressed**, a NASS county whose value
the provider withheld as `(D)`, and a county nobody published a row for were
all painted `CHOROPLETH_FALLBACK_COLOR` under one legend row reading **"No
observation"**. That label is false of the first two: there is an
observation, and the source published a word saying why it carries no number.

The distinction is the guide's own — "`value_status` says why in the source's
own vocabulary" — and it is the reason `ComposedArticle` exists, in its own
words: a reader "had no way to tell which". The map was the surface that
erased it, on exactly the measures where suppression is common (PLACES
small-cell estimates, NASS small-cell withholding).

## What was changed

- `CHOROPLETH_WITHHELD_COLOR`: a colour that is deliberately not in
  `CHOROPLETH_PALETTE`, so a withheld geography still cannot read as a
  quantity.
- `withheldGeographies` collects the rows that carry a status instead of a
  number, excluding any geography already plotted and any row that carries
  neither — a row with no value and no status is silence, and the fallback
  colour already says that.
- Its own legend row: `Value not published: <the source's own words>` with
  the count, above the fallback row. Both the linear and logarithmic models
  produce it, and so does the "nothing to scale" path — a page whose every
  row withheld its value is a published answer, not an empty one.
- A withheld geography still enters neither the scale (`minValue`,
  `maxValue`) nor `valueCount`.
- `comparison.ts`'s synthetic status is now `"not on both sides"`, the phrase
  that completes the legend's sentence, rather than a sentence of its own.

## A reviewed assertion changed, deliberately

WEB-002 asserted:

> A suppressed geography must not appear in the colour expression at all;
> leaving it out is what makes the map render it as no-data.

That mechanism is what erased the distinction, so the assertion is now the
stronger statement of the same intent: such a geography carries **no palette
colour** and does not touch the scale or the count. Both halves of the
original purpose — a withheld value never reads as a quantity, and never
drags the scale — are asserted explicitly rather than implied by absence.

## Validation

`tests/frontend/unit/explorer-contracts.test.js`:

- the suppressed geographies are painted the withheld colour, which is not
  in the palette, and `minValue` / `maxValue` / `valueCount` are unmoved
- the legend carries `Value not published: suppressed, missing` with a count
  of 2, beside `No observation` with no count
- a row with neither a value nor a status produces no withheld row and stays
  out of the expression
- a page whose only row withheld its value legends both rows and reports
  `valueCount: 0`

395 frontend unit tests and 94 browser specs pass.

## Deliberately not done

- **No per-status colour.** Three statuses would be three greys nobody can
  tell apart; the legend names the words and the count, which is what the
  swatch cannot carry (WEB-025's rule).
- **The statuses are trimmed at three** with an ellipsis rather than wrapped,
  because a legend row is one line and the count is what a reader needs
  first.
- **The extrusion model is unchanged.** A withheld value has no height, and
  giving it one would be inventing a quantity; the map's colour is where the
  distinction belongs.
