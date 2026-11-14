# CDC fixture source notes

These small fixtures are curated exact field/value excerpts from official CDC
Open Data responses retrieved on 2026-08-26. JSON indentation and field order
were normalized for review; source strings, identifiers, null/omitted fields,
numeric text, footnotes, and confidence limits were not reinterpreted.

- CDI metadata: `https://data.cdc.gov/api/views/hksd-2xuw`; ODbL; metadata
  watermark `rowsUpdatedAt=1780605223`.
- CDI observations: `https://data.cdc.gov/resource/hksd-2xuw.json`; selected to
  cover national overall, state stratified, confidence-interval, and missing
  provider states.
- PLACES county metadata: `https://data.cdc.gov/api/views/swc5-untb`; public
  domain; 2025 release; metadata watermark `rowsUpdatedAt=1764844506`.
- PLACES county observations: `https://data.cdc.gov/resource/swc5-untb.json`;
  selected to cover crude and age-adjusted modeled county estimates, a matching
  U.S. comparison estimate, and a provider-suppressed county row.

The metadata fixtures retain only the reviewed identity, watermark, license,
and consumed column/type contract. Observation fixtures contain only the
registered `$select` fields, matching production requests. They are not full
dataset downloads and must not be treated as current release counts.

- PLACES tract metadata: `https://data.cdc.gov/api/views/cwsq-ngmh.json`, read
  2026-11-12 by OFFICE_LAB probe 6; public domain; 2025 release; watermark
  `rowsUpdatedAt=1765560443`; `count(*)` answered 3,047,284. The fixture keeps
  the identity, watermark, licence and the full column contract.
- PLACES tract observations (`places_tract_dane_offset_{0,2000,4000}.json`):
  every Dane County row (`countyfips=55025`, 4,920 rows, 123 tracts x 40
  measures) of `https://data.cdc.gov/resource/cwsq-ngmh.json`, cut to the
  registered `$select` fields, ordered by the registered `$order` and split at
  2,000 rows, values unchanged.
