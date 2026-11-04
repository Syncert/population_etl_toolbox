# ADR-0014: Economic performance by party is a place's change net of the nation, never a score

- **Status:** Proposed 2026-11-04. Nick chose the direction the same day
  (option B of the elections proposal, with state legislatures added).
- **Date:** 2026-11-04
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** [`ELECTIONS_AND_ECONOMIC_PERFORMANCE_PLAN.md`](../plans/in_progress/ELECTIONS_AND_ECONOMIC_PERFORMANCE_PLAN.md);
  the discovery proposal <https://claude.ai/artifact/NJW5bJZ8RXiURwkZuGSBPH>
  (private to Nick); the tax and policy proposal
  <https://claude.ai/artifact/BJ5y3sdnDkRPhDVvickfUx>, which adds fiscal
  measures to the same views and may add a fiscal section to this ADR.
- **Builds on:** [ADR-0001](0001-data-layer-boundaries.md) (derived products
  are labelled, never written over provider facts),
  [ADR-0009](0009-per-capita-and-share-views.md) (provider rates first,
  margins travel with values), [ADR-0010](0010-cross-level-comparison.md)
  (what may be compared across levels) and
  [ADR-0011](0011-what-the-record-shows.md) (plain, sourced statements, no
  editorial).

## Context

Nick asked (2026-11-04, during the midterms) how the economies of places that
voted Republican or Democratic fared, with voting maps over the same years as
the economic maps and the history of which party held each House, Senate,
governor and state legislative seat.

Every place page already says "nothing here is a score or a ranking". Party
is the subject most likely to tempt a page into one. Four traps make a naive
comparison misleading:

- **National cycles.** A recession in a governor's term is mostly national.
  Raw change credits or blames the office holder for it.
- **Who leans which way.** Republican-leaning counties are mostly rural and
  Democratic-leaning counties mostly metropolitan, so an all-county
  comparison mostly measures rural against urban.
- **Reverse direction.** The economy moves the vote. Grouping places by a
  vote cast after the change measures that, not the reverse.
- **The ecological fallacy.** How a county voted says nothing about how its
  Republican or Democratic residents fared.

## Decision

1. **Net of the nation.** Every performance figure is the place's change over
   a window minus the nation's change over the same window, one measure at a
   time. Rates (unemployment, poverty) differ in points; levels (income,
   jobs, output) in percent growth. Dollar measures are real, deflated with
   the national PCE price index, and say so.
2. **Measures side by side, never summed.** No index, score, grade or rank
   that combines measures. A reader sees each measure in its own column or
   chart, with its source, cadence and margin where the source publishes one.
3. **Windows are declared.** Two kinds only:
   - *A term*: from the last full period (month for monthly series, year for
     yearly) before the term starts to the last full period inside it. A term
     still running is labelled "so far".
   - *A fixed period grouped by lean*: places are grouped by a vote cast at or
     before the window's start, never after it.
4. **Peer groups are offered and named.** The lean comparison can be
   restricted to places of the same USDA rural-urban type, the same state, a
   similar starting level of the measure, or a similar industry mix. The
   group in use is always stated on the chart.
5. **Groups show spread, not only a middle.** A lean group is shown as its
   middle place and the middle half of its places, with the count of places.
   Population weighting, if offered, is labelled.
6. **Seats are facts with dates and sources.** A seat term carries office,
   seat (class or district), holder, party as the source records it, caucus
   where it differs (independents), start and end dates, and its source.
   A county in more than one House district shows each district with its
   share of residents for that Congress.
7. **Standing wording.** Every view that pairs party with economics carries,
   in plain words: these are comparisons, not causes; office holders share
   effects with federal policy, the Federal Reserve and national trends; a
   county's vote does not describe its voters individually; people moving in
   and out change a place's averages.
8. **Party colours are fixed.** Red for Republican, blue for Democratic, grey
   for others, used only for party identity, never for good or bad. Better
   and worse than the nation use a separate, party-neutral pair.

## Consequences

- The site can answer "how did places that leaned one way fare, compared
  with similar places" for 2000 onward, and "how did this place do under each
  office holder" for every term since 1969 where the measures exist.
- Election results and seat terms become a source like any other, with
  registry, capture, silver, gold, DQ rules and catalog entries.
- A new derived relation (lean groups per window and measure) must be
  rebuilt when either elections or the economic sources publish.
- Some questions stay out of reach: why people voted as they did needs survey
  data (for example the Cooperative Election Study), which is not in scope.

## Alternatives considered

- **A composite "economic performance score" per party.** Rejected: it
  hides which measure drives the result and contradicts the site's rule.
- **Raw change without the nation removed.** Rejected: it credits national
  cycles to local office holders.
- **Grouping by the latest vote.** Rejected: it reverses cause and effect.
- **Regression-adjusted effects.** More rigorous, but a model's choices are
  hard for ordinary readers to check. Peer groups give most of the benefit
  in a form people can follow. Kept as a possible article, not a page.
