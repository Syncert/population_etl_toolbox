# Implementation plans

## Active architecture work

- [Warehouse data-quality assessment](./to_do/WAREHOUSE_DATA_QUALITY_PLAN.md) — source-to-warehouse completeness, lineage, reconciliation, freshness, validity, anomaly, and deployment-certification contracts.

- [Data-layer design remediation tickets](./completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md) — active prerequisite work for raw capture, control-plane separation, shared glossary ownership, and existing-source cutover.
- [CI/CD GitHub Actions migration](./completed/CICD_GITHUB_ACTIONS_MIGRATION_PLAN.md) — co-delivered workflow, packaging, coverage, bootstrap-parity, and release evidence migration for the data-layer remediation.

## Shared reference pipeline

- [Census geography reference pipeline](./completed/GEOGRAPHY_REFERENCE_PIPELINE_PLAN.md) — existing implementation audit and target versioned nation/state/county/place identity, attribute, geometry, and relationship dimensions.

## Planned source pipelines

- [Census Population Estimates Program](./to_do/CENSUS_PEP_PIPELINE_PLAN.md) — national, state, county, and incorporated-place population estimates with complete vintage history.
- [CDC illness and disease](./to_do/CDC_DISEASE_ILLNESS_PIPELINE_PLAN.md) — CDI national/state and PLACES county health observations.
- [FBI crime](./to_do/FBI_CRIME_PIPELINE_PLAN.md) — national/state/county and carefully qualified city-facing UCR observations with agency and participation coverage.
- [USDA NASS crop data](./to_do/USDA_NASS_CROP_PIPELINE_PLAN.md) — national/state/county crop acreage, yield, production, condition, price, and value observations.

## Planned API platform

- [API development](./to_do/API_DEVELOPMENT_PLAN.md) — versioned discovery, observation, comparison, reliability, security, and saved-analysis contracts over the completed warehouse. Dedicated work under this plan is blocked until every planned data-source pipeline in `to_do/` has been implemented, human-accepted, and moved to `completed/`.

## Planned web application

- [Web analytics foundation and first-wave products](./to_do/WEB_ANALYTICS_FIRST_WAVE_PLAN.md) — capability-driven catalog, explorer, comparison, profiles, saved analyses, evidence composition, and data-quality experiences. Feature development is blocked until the API plan and its frontend handoff are human-accepted into `completed/`.

## Delivery order

The source plans are approved planning artifacts, not permission to bypass the expansion gate. Recommended sequencing is:

1. Complete the expansion prerequisites in the data-layer remediation plan and its interdependent CI/CD migration plan.
2. Deliver geography GEO-001 through GEO-003 for versioned nation/state/county/place identity and attributes.
3. Deliver GEO-004 when place geometry/relationships or FBI agency bridges require it.
4. Implement one narrow new-source vertical slice, preferably PEP totals because it exercises the shared Census geography contract directly.
5. Implement CDC, USDA NASS, and FBI as independent adapters; FBI city-facing publication waits for the agency/geography bridge.
6. Expand datasets within each provider only after capture replay, release atomicity, geography reconciliation, and glossary publisher contracts pass.
7. Complete, review, and human-accept every planned data-source pipeline. Bounded source-specific API vertical slices may be delivered with those warehouse plans, but the cross-source API platform plan remains blocked while any data-source plan is in `to_do/`, `in_progress/`, or `needs_review/`.
8. Complete the warehouse-wide quality certification, then claim the API development plan and build only against stable publication contracts.
9. Complete and human-accept the API plan and its frontend contract handoff, then claim the web analytics first-wave plan. Publishing and social-hub work remains a separate follow-on plan.

## Geography depth summary

| Plan | National | State | County | City/place |
| --- | --- | --- | --- | --- |
| Shared geography | Yes | Yes | Yes | Yes |
| Census PEP | Yes | Yes | Yes | Yes |
| CDC illness/disease | Yes | Yes | Yes | No; initial scope stops at county |
| FBI crime | Yes | Yes | Yes | Yes, with provider geography or validated agency/place evidence |
| USDA NASS crops | Yes | Yes | Yes | No |

County and place are sibling Census geography branches beneath state because a place can intersect multiple counties. “City” is a product label for qualifying canonical places, not a generic string or an assumption that every local provider entity is a city.
