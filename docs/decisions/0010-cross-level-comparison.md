# ADR-0010: Comparing places across geographic levels

- **Status:** Accepted 2026-10-26 by the repository owner (Nick); proposed the same day.
- **Date:** 2026-10-26
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** v1.0 build-out items A-4 (`feat/api-cross-level-compare`)
  and D-7 (`feat/web-cross-level-compare`).
- **Builds on:** [ADR-0009](0009-per-capita-and-share-views.md) (per-capita
  and share views) and the comparison policy already served by
  `/comparison/preflight`.

## Context

The comparison API accepts one grain at a time. Readers ask cross-level
questions -- "how does Austin compare with Travis County?", "Dane County with
Wisconsin?", "the Austin metro with the nation?" -- and the site today refuses
county versus state and shows city versus county misleadingly (two counts of
very different populations, side by side as if comparable).

Some cross-level comparisons are sound and some are not:

- a median, an average, a rate or a share describes its population whatever
  its size, so a city's median household income beside its county's is a
  fair comparison;
- a count is not: a county has more people in poverty than a city inside it
  by construction;
- a value from a different release, period or program on each side compares
  different things;
- a city that is part of its county overlaps it, so a "difference" between
  them is partly the city compared with itself.

## Decision

1. **Mixed grains are accepted.** The comparison API accepts any two
   geographies at the levels the site serves (nation, state, county, metro,
   city, tract) for one measure, and answers with both values.
2. **Comparability is decided per measure kind, and stated.**
   - Medians, means, rates, shares and indexes compare across levels.
   - Counts do not: a count comparison across levels is refused with reason
     `counts_differ_in_size` and a pointer to the measure's per-capita or
     share view where an approved ADR-0009 pairing exists.
   - The measure's kind comes from its reviewed definition
     (`docs/semantics/metric_definitions.json`, which gains a `kind`); a
     measure with no reviewed kind is treated as a count, the conservative
     default.
3. **Same program and period, or said otherwise.** Both sides must come from
   the same program; a different period on each side is shown with both
   periods named and the reason `periods_differ`, and never aligned silently.
   A side with no published value is "not published", never zero.
4. **Margins of error decide "different".** For survey estimates the answer
   says whether the two 90 percent intervals overlap. Overlapping intervals
   are reported as "not distinguishable at 90 percent confidence"; no
   significance test beyond that is computed, and no "higher" or "lower"
   verdict is printed when the intervals overlap.
5. **Containment is disclosed.** When one geography contains or overlaps the
   other (from the reference's relationships), the answer says so and names
   the share of the smaller place inside the larger. No difference is
   computed between a place and its own container as if they were
   independent; the values are shown side by side.
6. **No verdicts.** The response carries values, periods, intervals and
   reasons. It never ranks, scores or names a "winner", and the web prints
   the same.

## Consequences

- City versus county, county versus state and metro versus nation work for
  every median, rate and share, and for counts through their per-capita view.
- A refusal is always explained and points at what would answer the
  question.
- The comparison preflight's existing rules (same source analysis readiness,
  period alignment) keep applying; this adds the cross-level rules beside
  them.
