# ADR-0009: Per-capita and share views are a declared derived product

- **Status:** Accepted 2026-10-26 by the repository owner (Nick); proposed the same day.
- **Date:** 2026-10-26
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** v1.0 build-out items A-3 (`feat/api-per-capita-and-shares`)
  and D-6 (`feat/web-per-capita-toggle`).
- **Builds on:** [ADR-0001](0001-data-layer-boundaries.md) (derived products
  are labelled and never written over provider facts) and
  [ADR-0007](0007-derived-time-aggregates.md) (a reviewed method per metric,
  complete inputs only). ADR-0007 covers time, not ratios between two
  measures, so this decision is needed.

## Context

Place pages show counts: people in poverty, uninsured children, reported
violent offenses, housing units. A reader comparing a city of 30,000 with a
county of 1,000,000 needs "per 1,000 residents" or "share of households",
which no provider publishes for most of these counts. Dividing two numbers is
easy and easy to get wrong:

- the numerator and denominator can describe different universes (people in
  poverty are counted among people whose poverty status is determined, not
  all residents);
- they can describe different periods (a 2019–2023 survey count over a 2024
  population estimate);
- they can come from different programs with different methods (ACS counts
  over a PEP population);
- an ACS ratio has a margin of error the Bureau tells users how to compute,
  and dropping it presents a survey estimate as exact;
- a provider sometimes publishes the rate itself (FBI's rate per 100,000,
  PEP's rates per 1,000, SAIPE's poverty percentage), and a recomputed rate
  beside it invites the reader to pick whichever suits them.

## Decision

1. **A reviewed pairing per derived view.** A versioned semantic record
   (`docs/semantics/derived_ratios.json`) binds each derived view to a
   numerator metric, a denominator metric, a kind (`per_capita` with its
   scale -- per 1, per 1,000 or per 100,000 -- or `share`), the universe the
   result describes, owner, reviewer, status and citations. Only `approved`
   entries are served; `draft` entries are proposals. Nothing is paired by
   inference from names or units.
2. **Same program, same period, same geography.** A derived value is computed
   only when numerator and denominator come from the same program and the
   same release (an ACS 5-year count over the same release's ACS total, never
   over PEP), the same period and the same geography. Otherwise the view is
   unavailable for that row and says why (`different_period`,
   `denominator_missing`, `denominator_zero`, `suppressed_input`).
   Suppressed, missing or withheld inputs make the result missing, never zero.
3. **The share's denominator is the numerator's universe.** A share uses the
   table total that defines the numerator's universe (people below poverty
   over people whose poverty status is determined, B17001_002 / B17001_001),
   not the total population. A `per_capita` view over total population is a
   different, separately named product and is offered only where the
   numerator's universe is all residents.
4. **Margins of error follow the Bureau.** For ACS inputs the derived value
   carries a margin of error computed with the Census Bureau's published
   formulas for derived proportions and ratios (*Understanding and Using
   American Community Survey Data*, chapter 8): the proportion formula when
   the numerator is a subset of the denominator, the ratio formula when it is
   not (and when the proportion formula's radicand is negative). Inputs
   published with a controlled margin (the Bureau's `*****`) are treated as
   zero margin, as the Bureau instructs.
5. **Provider rates take precedence.** Where the provider publishes the rate
   (FBI UCR rate per 100,000, PEP rates per 1,000, SAIPE and SAHIE
   percentages, CDC PLACES prevalence), that is what the per-capita view
   shows, labelled as the provider's; no recomputed value appears beside it.
6. **Labelled in every response.** A derived row carries `derived: true`,
   the pairing's identity and version, the numerator's and denominator's
   metric codes, release and period, the universe, and the method of its
   margin of error. The web names the denominator beside every per-capita or
   share value ("per 1,000 residents, ACS 2019–2023 total population").
7. **Computed in the API over served rows.** Like ADR-0007's trailing
   windows, a derived ratio is computed at request time from served provider
   rows; nothing is written to gold, and no ratio becomes a provider fact.

## Consequences

- The per-capita and share toggles (D-6) can only offer what an approved
  pairing allows, so the first release covers the place pages' ACS counts
  and FBI/PEP provider rates; others are added one reviewed pairing at a
  time.
- A reader can always see what was divided by what, from which release, and
  how uncertain the result is.
- Composite scores remain out of scope: a ratio of two measures is not an
  index, and nothing here sums unlike quantities.

## Alternatives considered

- **Per-capita over PEP population for every count.** Rejected: it mixes
  programs and periods, and the survey's own total is the denominator its
  counts were weighted to.
- **Computing ratios in the web client.** Rejected by ADR-0001 and AGENTS.md:
  the rule would live where no other consumer can reuse or review it.
