# Declined source: CDC WONDER mortality and natality

## Decision

Declined on 2026-10-06 by the second-tier source scouting plan. No adapter
plan is filed under `docs/plans/to_do/`.

The [Place Almanac website plan](../PLACE_ALMANAC_WEBSITE_PLAN.md) lists CDC
WONDER mortality and natality as a county, annual source for the Health and
People chapters. The provider's own documentation rules out the automated,
county-grain, replayable ingestion that the
[adapter checklist](../../reference/ADDING_A_DATA_SOURCE.md) requires, through
every permissible channel.

## Findings

1. **The WONDER API serves national vital statistics only.** The API
   documentation, under "More about WONDER API queries", states that "only
   national data are available for query by the API. Queries for mortality
   and births statistics from the National Vital Statistics System cannot
   limit or group results by any location field, such as Region, Division,
   State or County."
   Source: <https://wonder.cdc.gov/wonder/help/wonder-api.html>
2. **County figures exist only in the interactive web application.** The
   same restriction applies to the API only, and county grouping remains
   available to people using the web forms. Scripting those forms to get
   county rows would get around a restriction the provider deliberately
   applies to automated access. The repository will not do that.
   Source: <https://wonder.cdc.gov/wonder/help/wonder-api.html>
3. **The downloadable public-use files have no county identifiers.** The
   NCHS data release policy states: "All public-use micro-data files from
   2005-present contain individual-level vital event data at the national
   level only. Specifically, these files contain no geographic identifiers at
   the state, county, or city level." County-identified files are restricted
   files that require NCHS approval of a research request and a signed NCHS
   Data Use Agreement. They do not fit a public warehouse that republishes
   rows.
   Sources: <https://www.cdc.gov/nchs/nvss/dvs_data_release.htm>,
   <https://www.cdc.gov/nchs/nvss/nvss-restricted-data.htm>
4. **Natality county coverage is partial even on the web.** Only counties
   with 100,000 or more persons (2010 Census for 2014-2024 data) are
   identified. Smaller counties are combined under "Unidentified Counties"
   for the state, so an every-county births card could not be built anyway.
   Source: <https://wonder.cdc.gov/wonder/help/natality.html>

## Constraints that would bind any future use

These are recorded so that a later reconsideration starts from verified
facts:

- Data use restrictions: "Do not present or publish statistics representing
  nine or fewer births or deaths, including rates based on counts of nine or
  fewer births or deaths"; use "for statistical reporting and analysis only";
  "make no attempt to learn the identity of any person or establishment".
  Source: <https://wonder.cdc.gov/datause.html>
- Suppression: counts below ten are suppressed, and so are population
  figures below ten. Rates are flagged "Unreliable" when the death count is
  below 20. Source: <https://wonder.cdc.gov/wonder/help/ucd.html>
- Age adjustment: the default standard is the year 2000 U.S. standard
  population. The 1940, 1970 and WHO standards can also be selected.
  Source: <https://wonder.cdc.gov/wonder/help/ucd.html>
- API mechanics (national only): POST to
  `https://wonder.cdc.gov/controller/datarequest/<database ID>` with an XML
  `request_xml` parameter. `accept_datause_restrictions=true` is required.
  The provider asks for one query about every two minutes, run one at a
  time. Source: <https://wonder.cdc.gov/wonder/help/wonder-api.html>

## What would reopen this

- NCHS publishes a county-level aggregate product whose terms allow automated
  download and republication, with suppression applied by the provider. Any
  such candidate must be verified against its own official terms before a
  plan is filed. None was verified during this scouting pass.
- The owner chooses a restricted-use path under an NCHS Data Use Agreement.
  That is a governance decision outside the public-warehouse model and is not
  assumed here.

## Unverified items

- The exact suppression wording on the Underlying Cause of Death help page
  ("0-9" versus "one to nine") was read inconsistently. The data use
  restrictions page wording ("nine or fewer") is the one cited above.
- The current year ranges and release cadence of the WONDER mortality
  databases (final and provisional) were not checked, because the source was
  declined.
