# ADR-0012: A county crime rate from the FBI's own agency figures

- **Status:** Accepted 2026-10-31 by the repository owner (Nick), with a 50 % coverage floor
  and the reported population visible on every county rate; proposed the same day.
- **Date:** 2026-10-31
- **Decision owners:** Repository owner and data-product maintainers
- **Amends:** [ADR-0009](0009-per-capita-and-share-views.md). This adds one
  derived product that rule 5 did not foresee. It does not loosen rule 2.
- **Builds on:** the county crime roll-up
  (`docs/plans/needs_review/COUNTY_CRIME_ROLLUP_FROM_AGENCY_REPORTS_PLAN.md`,
  `gold_fbi.county_rollup`) and D-6's per-capita toggle (#183).

## Context

The per-capita toggle was asked to show reported violent offenses per
resident for a county beside its state. The FBI does not publish that figure:

- **The FBI publishes rates only for states and the nation.** These are rates
  per 100,000, which ADR-0009 rule 5 already serves as the provider's own.
- **The county roll-up is counts only, and it double-counts.**
  `gold_fbi.county_rollup` sums reported agency totals, and an agency serving
  several counties contributes its whole count to each of them. The roll-up
  says so itself (`includes_multi_county_agency`).
- **Two programs are off limits by rule 2.** Dividing FBI counts by a Census
  population mixes programs and periods, and ADR-0009 rule 2 forbids it. It
  would also be wrong even where it is tempting. A county whose agencies did
  not all report would read as low-crime, not as under-reported.

The FBI computes its own state rate from inside its program. The numerator is
the offenses that reporting agencies reported. The denominator is the
population those same agencies cover, as the FBI estimates it per agency.
Every agency's participation row in this warehouse carries that population
(`silver_fbi.participation_revision.population`). So a county rate can be
computed the FBI's way, from one program, with no Census figure involved.

## Decision

1. **A county rate is computed the way the FBI computes a state rate.** For a
   county and a month, take the agencies linked to that county that reported
   that month. The rate is their summed reported offenses, divided by their
   summed FBI-published agency population, times 100,000.
   - Numerator and denominator are both FBI figures from the same release and
     the same month. ADR-0009 rule 2 holds unchanged.
   - It is per 100,000, the same scale as the FBI's state and national rates,
     so a county and its state read on one scale.
   - It is monthly, like the agency reports it sums. It is never rolled up
     into a year (the project rule against rolling monthly measures up to
     annual).
2. **Agencies that serve more than one county are left out of both sides.**
   The FBI gives such an agency one population and one count for its whole
   jurisdiction, with no split by county. So it can be put in no single
   county's numerator or denominator without inventing a split. Leaving it
   out keeps every included offense matched to the population it was counted
   among. The excluded agencies are listed with the result.
3. **The result carries its coverage, and refuses when coverage is thin.**
   Each county-month row publishes:
   - the contributing agencies;
   - the agencies excluded as multi-county;
   - the agencies linked to the county that did not report that month;
   - the covered population (the reporting agencies' FBI population);
   - the linked population (every single-county linked agency's FBI
     population for that month, reporting or not);
   - the coverage share, which is covered population over linked population.

   Coverage is measured inside the FBI program, so no Census figure enters
   it either.

   The rate is unavailable with a reason, never zero, in three cases:
   - no linked agency reported (`no_reporting_agency`);
   - every reporting agency was multi-county (`only_multi_county_agencies`);
   - the coverage share is under 50 % (`coverage_below_floor`).

   Every county rate shows its coverage where the reader sees the figure:
   "covers N people, X % of the population its agencies serve". A refused
   month shows the same numbers, so the reader can see why it was refused.
4. **It is derived and named as such.** The pairing is one draft entry in
   `docs/semantics/derived_ratios.json` with kind `agency_rate`. It is served
   through `/observations/derived` like every other pairing. It is labelled
   with the following:
   - `derived: true`;
   - the unit "per 100,000 people covered by reporting agencies in this
     county";
   - the method.

   It is never shown as an FBI-published rate, and the county card keeps the
   words "computed from agency reports" beside the figure. Like every
   pairing, it is served only once approved, or locally with
   `DERIVED_RATIOS_SERVE_DRAFTS=1`.
5. **It is computed in gold, beside the roll-up.** A
   `gold_fbi.county_agency_rate` view reads `gold_fbi.county_rollup`'s inputs
   (agency observations and the agency–county links) and the agency
   participation rows. The API then serves rows; it does not divide. This
   matches the roll-up's own warehouse-first boundary. ADR-0009 rule 7
   ("computed in the API over served rows") is amended for this one product,
   because the input it needs is agency rows, which the API does not serve.

## Consequences

- A county page can show "violent offenses per 100,000 people covered by
  reporting agencies" beside the FBI's state rate on the same scale. Both
  figures are FBI-derived, and the reader is told which one was computed here.
- **Counties served mostly by a sheriff or state police** have FBI agency
  populations that exclude the towns that report separately. Their rate
  describes the covered population, which the coverage fields make visible.
  It is not the county's whole population.
- **Thinly reporting counties** show a refusal or a small covered population,
  not a misleadingly low rate.
- **Data quality:** one new rule checks that the county rate's numerator and
  denominator sum the same agency set. This is an executable check beside
  DQ-FBI-008.
- **What it does not give:** a county rate is not comparable with a Census
  per-capita figure, and nothing here invites that comparison.

## Alternatives considered

- **Divide county counts by the Census population (ACS or PEP).** Rejected.
  It breaks ADR-0009 rule 2. It double-counts multi-county agencies. It also
  turns non-reporting into apparent safety.
- **Split multi-county agencies by Census population share.** Rejected for
  now. It invents a split the FBI does not publish, and it brings the Census
  back in through the allocation. It can be revisited as its own ADR if
  excluded agencies leave many counties with a refusal.
- **Keep the state rate only (status quo).** This was the recommended option
  on the D-6 card. Nick chose this amendment instead on 2026-10-31.

## Settled at acceptance

1. **Coverage floor:** 50 %, as rule 3 states. A county-month under it is
   refused, and the refusal shows the coverage.
2. **Crime types:** violent crime and property crime, the two FBI products
   with state rates. Other products follow one reviewed pairing at a time.
