# ADR-0011: The wording rules for "What the record shows"

- **Status:** Proposed 2026-10-26, awaiting the repository owner (Nick).
- **Date:** 2026-10-26
- **Decision owners:** Repository owner and data-product maintainers
- **Related work:** v1.0 build-out item D-9 (`feat/web-check-the-record`).
- **Builds on:** the almanac plan's guardrails, the top-20 use case
  guardrails, ADR-0007 and ADR-0009.

## Context

Each place page will answer a small set of common, measurable questions --
"Has violent crime gone up here in ten years?", "Are rents rising faster than
incomes?", "Is the population growing or shrinking, and why?", "How does
unemployment here compare with the state?" -- with a chart and one plain
sentence generated from the data. Generated sentences about crime, money and
migration are where a neutral site can most easily stop being neutral: a verb
choice ("soared", "plummeted"), an implied cause, a dropped caveat or a
missing agency report turned into "crime fell".

## Decision

1. **Questions are measurable and fixed.** Each question is a reviewed
   template naming its series, its comparison (change over a stated span, or
   one place beside another) and its caveats. No question names a person, a
   party, an official, a policy or a claim someone made. Questions are
   phrased as a reader would ask them, never as an accusation.
2. **Sentences are templates over numbers.** The sentence is built from a
   fixed template: the measure, the place, the two periods, the two values,
   the change in the measure's own unit (and percent where the unit allows),
   and the source. Allowed verbs are "rose", "fell", "was unchanged"
   (change within the survey's margin of error, or zero), "was higher than",
   "was lower than" and "was not distinguishable from" (intervals overlap).
   No adjectives of degree ("sharply", "dramatically"), no causal words
   ("because", "due to", "driven by", "led to"), no evaluative words
   ("better", "worse", "safer", "dangerous").
3. **Uncertainty decides the verb.** For a survey estimate, a change whose
   two 90 percent intervals overlap is "was not distinguishable from"; only
   non-overlapping intervals get "rose" or "fell".
4. **Coverage is part of the answer.** For crime the sentence states how many
   agencies reported in each period (and the share of the population they
   cover, as the FBI publishes it); when coverage differs by more than the
   template's threshold between the two periods, the sentence says the change
   may reflect reporting and gives both coverages. A period with no reporting
   agencies is "not reported", never zero crime.
5. **Two series are never one.** "Are rents rising faster than incomes?"
   states each series' change separately, from the same program and periods
   (ACS median gross rent and median household income, same releases), and
   compares the two percentages; it never divides one by the other into an
   "affordability index".
6. **Components, not causes.** "Why is the population changing?" lists the
   published components (births, deaths, domestic and international
   migration) as the Population Estimates Program publishes them; it never
   attributes the change to anything else.
7. **Every answer shows its working.** The chart, the series' sources, the
   periods, the coverage and the caveats appear with the sentence, and the
   sentence links to the data it was made from.

## Consequences

- The section can be extended one reviewed question at a time.
- A reader can check every sentence against the chart beside it.
- Some questions will often answer "not distinguishable" or "not reported";
  that is the record, and it is shown as such.
