---
title: Why does a survey estimate have a margin of error?
summary: The American Community Survey asks a sample of households, so every estimate comes with a range that a different sample could have produced.
caveat_keys: [margin-of-error]
metric_codes: [CENSUS_ACS:acs5:B19013_001, CENSUS_ACS:acs5:B01003_001]
sources: [CENSUS_ACS]
example_metric: CENSUS_ACS:acs5:B19013_001
reviewed: 2026-10-06
reviewer: Agent draft; editorial review pending
---

## Short answer

The American Community Survey does not ask every household. It asks a sample and weights the answers up to the whole population. A different sample would have given a somewhat different number, and the margin of error says how different.

The Census Bureau publishes margins of error at the 90 percent confidence level. The estimate plus or minus its margin gives a range that, nine times out of ten, contains the value a full count would have found. Smaller places have smaller samples and wider margins.

## What it is not

- It is not a mistake or a correction. The estimate is still the best single number.
- It does not cover every kind of error. People misremembering their income, or not answering, are not in the margin.
- Two estimates whose ranges overlap are not proven different. A county that appears to rank above another may not.

## Worked example

A median household income of $78,000 with a margin of ±$1,200 is a range of $76,800 to $79,200. A neighboring county at $77,500 ± $2,000 cannot be said to have a lower median.

## Where it is used

Every ACS value on a place page shows its margin beside it, as do the profile cards and the explorer's tables and exports.
