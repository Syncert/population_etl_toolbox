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
  selected to cover crude and age-adjusted modeled estimates plus a provider-
  suppressed county row.

The metadata fixtures retain only the reviewed identity, watermark, license,
and consumed column/type contract. Observation fixtures contain only the
registered `$select` fields, matching production requests. They are not full
dataset downloads and must not be treated as current release counts.
