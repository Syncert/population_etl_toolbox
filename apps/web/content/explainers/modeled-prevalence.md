---
title: Why is a modeled prevalence not a case count?
summary: CDC PLACES estimates the share of adults with a condition from a national survey and a statistical model, not from medical records.
caveat_keys: [modeled-prevalence]
metric_codes: [CDC:places_county:OBESITY:AgeAdjPrv, CDC:places_county:DIABETES:AgeAdjPrv]
sources: [CDC]
example_metric: CDC:places_county:OBESITY:AgeAdjPrv
reviewed: 2026-10-06
reviewer: Agent draft; editorial review pending
---

## Short answer

The Behavioral Risk Factor Surveillance System asks adults by telephone about their health. It has too few respondents in most counties to publish county figures directly, so CDC PLACES uses a statistical model, combining the survey with each county's age, sex, race, and other population characteristics, to estimate how common a condition is likely to be there.

The result is a percentage of adults, with a confidence interval, and usually age-adjusted so places with older or younger populations can be compared.

## What it is not

- It is not a count of diagnoses, hospital visits, or deaths.
- It is not measured in the county. Two similar counties can receive similar estimates because the model expects them to, whatever happened locally.
- An age-adjusted and a crude prevalence are different measures. This site shows which one it is and never converts one to the other.

## Worked example

An age-adjusted obesity prevalence of 31.8 percent with an interval of 30.1 to 33.4 percent says about one adult in three is likely obese, given who lives there. It is not a tally of people.

## Where it is used

The Health chapter of county place pages, the disease and illness use cases, and the profile cards.
