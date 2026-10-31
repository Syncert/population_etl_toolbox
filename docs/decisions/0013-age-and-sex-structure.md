# ADR-0013: Age and sex structure is regrouped from the provider's own table

- **Status:** Accepted 2026-10-31 by the repository owner (Nick), with the
  comparison chosen by a toggle across enclosing levels (rule 4); proposed
  the same day.
- **Date:** 2026-10-31
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** [`AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md`](../plans/to_do/AGE_AND_SEX_STRUCTURE_VIEWS_PLAN.md)
  (discovery plan for population pyramids and age analytics on the site).
- **Builds on:** [ADR-0001](0001-data-layer-boundaries.md) (derived products
  are labelled and never written over provider facts),
  [ADR-0007](0007-derived-time-aggregates.md) (a reviewed method, complete
  inputs only), [ADR-0009](0009-per-capita-and-share-views.md) (shares,
  margins of error, provider rates first) and
  [ADR-0010](0010-cross-level-comparison.md) (shares compare across levels,
  counts do not).
- **Amends:** ADR-0009. Its pairings divide one published line by another.
  A pyramid band, a broad age group and a dependency ratio are *sums of
  several lines* of one table, which ADR-0009 did not foresee.

## Context

The warehouse holds the ACS sex-by-age table (B01001) at every grain the
site serves (nation, state, county, place, metro, tract), every release from
2005 (1-year) and 2009 (5-year). Shown one band at a time on a map, those 46
lines say little. The established reading of an age distribution is the
population pyramid: horizontal bars by age band, men on one side and women on
the other. Building one, and the summary numbers that go with it, means
arithmetic the provider did not do:

- B01001's 23 bands per sex are **unequal** (`20`, `21`, `22 to 24`,
  `60 and 61`, `62 to 64`, `65 and 66`, `67 to 69`). Drawn as published, the
  one-year bands look like a dent in the population and the three-year band
  like a bulge. A pyramid needs equal bands.
- The broad groups readers ask about (children, working age, 65 and over,
  85 and over) and the dependency ratios built on them are sums of bands.
- Each band is a survey estimate with its own margin of error; a sum has a
  margin the Bureau tells users how to approximate.
- Comparing a city's pyramid with its state's only works as shares of each
  place's own total; counts differ in size by orders of magnitude.
- Following one generation forward ("the people aged 25 to 29 in 2014 are
  30 to 34 in 2019") is a synthetic cohort, not a panel: the change is births
  abroad, deaths, moves and sampling error together, and two overlapping
  5-year releases share most of their respondents.

## Decision

1. **A reviewed band scheme, not inference.** A versioned semantic record
   (`docs/semantics/age_bands.json`) names, per source table, which published
   lines make each output band, for each sex. The first scheme is the
   standard eighteen five-year bands (`0-4` ... `80-84`, `85+`) from B01001
   and its 2000 census equivalent (SF1 P012, which publishes the same 23
   bands). Lines are summed only as declared there; nothing is grouped by
   reading labels. Owner, reviewer, status and citations as in ADR-0009.
2. **Same table, same release, same place.** A band, group or ratio is
   computed only from lines of one table in one release for one geography.
   A missing or suppressed line makes every band that needs it missing and
   says so (`suppressed_input`, `line_missing`); it is never read as zero,
   and a pyramid with a missing band is drawn with that band visibly absent.
3. **Margins of error follow the Bureau.** A sum's margin is the square root
   of the sum of its lines' squared margins (*Understanding and Using ACS
   Data*, chapter 8); a share of the place's total uses ADR-0009's
   proportion formula; a ratio uses its ratio formula. Controlled margins
   (`*****`) count as zero, as the Bureau instructs. The census years carry
   no margin and say so.
4. **Shares for shape, counts for size.** A pyramid is drawn as each band's
   share of the place's total population (both sexes), so a city and its
   state can be overlaid. Counts are shown in the tooltip and table. Under
   ADR-0010, shares compare across levels; counts are never overlaid.
   The comparison is the reader's choice among the place's enclosing levels,
   next level up by default: a city or tract against its county, its state
   or the nation; a county against its state or the nation; a state against
   the nation; a metro against the nation, or its state when it lies in one.
   A city spanning counties is compared with the county holding the largest
   share of its area, as the geography reference records it, and that share
   is stated.
5. **Summary indicators, defined once.** Served with these definitions:
   - *Median age*: the provider's own (B01002, census P013), never
     re-estimated from bands (ADR-0009 rule 5).
   - *Child dependency ratio*: people under 18 per 100 people 18 to 64.
   - *Old-age dependency ratio*: people 65 and over per 100 people 18 to 64.
   - *Total dependency ratio*: the sum of the two.
   - *Sex ratio*: men per 100 women, overall and per band.
   These match the Bureau's own S0101 subject-table definitions (under 18,
   18 to 64, 65 and over), so a reader checking data.census.gov finds the
   same number within rounding.
6. **Cohorts move by whole bands across non-overlapping periods only.** A
   cohort view follows a five-year band forward five years, and is served
   only between releases whose periods do not overlap: 1-year estimates five
   years apart, 5-year estimates whose windows do not share a year
   (2009–2013, 2014–2018, 2019–2023), or a census year. Anything else is
   refused with `overlapping_periods`. A place whose boundaries changed
   between the two periods (tracts across 2010 and 2020) is refused with
   `boundary_changed` unless the comparable-tract relation covers it. The
   response says in words that the change mixes deaths, moves and sampling
   error, and draws no conclusion.
7. **Labelled in every response.** A derived row carries `derived: true`,
   the scheme's identity and version, the input lines, the release and
   period, and the margin method, as ADR-0009 rule 6 asks.
8. **Where it is computed.** For one place and its parents, in the API at
   request time from served rows (ADR-0009 rule 7). The grain-wide summary
   indicators that a map or a ranking reads (old-age dependency ratio for
   every county in a state, say) may be pre-computed into a labelled derived
   gold relation built from the same served rows in the same publish step,
   because a map must not sum 46 lines for thousands of places per request.
   Such a relation is a derived product under ADR-0001: it is rebuilt from
   gold, never edited, never mistaken for a provider fact, and it serves
   exactly what the request-time method would.

## Consequences

- The site can draw a pyramid for any place the ACS covers, back to 2005,
  and to 2000 once the census table is crosswalked, with honest margins.
- Maps keep summary indicators (median age, share 65 and over, dependency
  ratios); single age bands stop being offered as maps.
- Other sex-by-age or by-age tables (poverty, health insurance, disability,
  mobility, veterans) can use the same scheme record, one reviewed entry at a
  time, for age-specific rates.
- One more derived relation exists to keep in step with the ACS publish, if
  rule 8's pre-computation is taken.

## Alternatives considered

- **Draw the 23 published bands as they are.** Rejected: unequal widths
  misdraw the shape. (Drawing them with bar height proportional to band width
  and values per year of age is honest but unfamiliar to most readers.)
- **Ingest the Bureau's S0101 subject table for the ratios.** Its ratios are
  provider-published, which ADR-0009 rule 5 prefers, but the summary
  indicators appear only in recent releases (from about 2017), the subject endpoint is a new ingestion
  path, and the history before 2017 would need this ADR's method anyway.
  Kept as an option in the plan's open questions.
- **Compute in the web client.** Rejected by ADR-0001 and AGENTS.md.
- **Use PEP's characteristics files instead of the ACS.** Better for annual
  county change (no sampling error), but they do not reach tracts or most
  cities. The plan proposes both, labelled by
  program; they are never mixed in one pyramid.
