# CDC FluView ILINet fixtures

Read on 2026-11-12 from FluView Interactive's download service with the
toolbox's own client identity, one request a second apart (outbreak probe 12):

- `GET https://gis.cdc.gov/flu2/GetPhase02InitApp?appVersion=Public`
- `POST https://gis.cdc.gov/flu2/PostPhase02DataDownload` with the page's own
  body for ILINet at the national (`RegionTypeId` 3), HHS region (1) and state
  (5) levels, every season the init lists (states: every season it marks as
  having state data).

The files are trimmed to keep the repository small; every kept line is CDC's
text, unchanged:

- `fluview_init.json`: the `seasons` entries for seasons 49, 64 and 65 (49 has
  no state data), every `hhsregion` entry, and the `states` entries for
  Wisconsin, New York, New York City, California, Los Angeles and Puerto Rico,
  each reduced to the keys the loader reads. Contact fields are left out.
- `ilinet_national.zip`: CDC's title and header lines and the national rows of
  2024 week 52 and 2025 weeks 1 and 2.
- `ilinet_hhs_region.zip`: the title, header and rows of Regions 2 and 5 for
  2025 weeks 1 and 2 (CDC leaves unpublished age cells empty here).
- `ilinet_state.zip`: the title, header and rows of California, New York, New
  York City, Puerto Rico and Wisconsin for 2025 weeks 1 and 2 (CDC writes `X`
  for figures it does not publish by state). The state download carried no
  Los Angeles rows on the probe date.

Each zip holds one `ILINet.csv`, rewritten with a fixed timestamp.
