# Vehicle registrations fixtures

Read on 2026-11-17 by the vehicle-fleet-tables source probe (plain GETs,
toolbox user agent) from the publishers' own sites.

- `veh-info.html` — WisDOT's vehicle-information page
  (`/Pages/about-wisdot/newsroom/statistics/veh-info.aspx`), trimmed to its
  first line and the one table row that links Report 28 (calendar year 2025
  and fiscal year 2026).
- `veh-info-prev.html` — WisDOT's previous-years page, trimmed to its first
  line and one link that is not a Report 28 (the real page links 2015-2024),
  so a test reads one calendar year.
- `rpt-28-cal-25-dane-and-state.pdf` — pages 1, 18-20 and 99-102 of
  `rpt-28-cal-25.pdf` (98 pages of Table 1, 3 of Table 2, then Table 3),
  cut with `pypdf` and otherwise unchanged: vehicles with no county, all of
  Dane County, the start of Dodge County (the page Dane ends on), every county
  of Table 2 with the statewide Sum, and Table 3's first page, where the read
  stops. Dane's printed Sum is 56 AUCY, 152,184 AUTO, 19,503 CYCL, 39,988
  TRLR, 318,345 TRUK, 530,076 in all; the state's is 6,384,450.
- `mv1-2024.xlsx`, `mv1-2023.xlsx` — FHWA Highway Statistics Table MV-1 for
  2024 and 2023 (`/policyinformation/statistics/<year>/xls/mv1.xlsx`), whole.
  2023 holds FHWA's estimates stored with decimals; Wisconsin 2024 is
  5,836,776 motor vehicles in all.
