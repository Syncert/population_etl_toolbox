# Adding a data source

Do not onboard another production source until the expansion gate in the [data-layer remediation plan](../plans/completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md) is satisfied. When the gate opens, every adapter must follow [ADR-0001](../decisions/0001-data-layer-boundaries.md).

Begin with the [source-adapter starter](../templates/source-adapter/README.md), which provides the expected package layout, a `config.py` template, initial milestones, and an agent handoff block.

## Adapter checklist

- [ ] Register a stable, provider-neutral source identity without editing a closed provider enumeration.
- [ ] Create `src/data_ingestion_toolbox/<source>/config.py` from the starter and define source scope, endpoint, timeouts, concurrency, Airflow pool, and PostgreSQL connection ID without performing I/O at import.
- [ ] Name and document the API-key environment variable; add only an empty placeholder to tracked environment examples and put the real value in an ignored local environment or deployment secret store.
- [ ] Validate credentials when the request executes, not during module/DAG import, and prove secrets cannot enter payload captures, request fingerprints, logs, or exception messages.
- [ ] Define the endpoint, request fingerprint, media type, schema/version identifier, and sensitive-data handling.
- [ ] Persist and commit an append-only, lossless response capture before parsing; include checksum, retrieval time, HTTP metadata, and run lineage.
- [ ] Put attempts, slices, retries, watermarks, errors, and quarantine status in the control plane, not the raw schema.
- [ ] Implement offline capture-to-silver-revision replay from a checked-in representative fixture.
- [ ] Perform parsing, typing, null interpretation, reshaping, deduplication/revision selection, validation, and derived identifiers in silver.
- [ ] Publish only deterministic, data-derived facts in gold.
- [ ] Expose the versioned glossary publisher contract without creating, altering, dropping, or seeding `gold_glossary` objects.
- [ ] Keep definitions, approvals, dashboard defaults, ownership commentary, aliases, and user/team preferences outside source ETL and gold data-product tables.
- [ ] Add checked-in fresh-bootstrap SQL, constraints, validation queries, and reset/re-ingestion instructions for any discarded beta data.
- [ ] Add unit, contract, integration, replay, malformed-payload/quarantine, bootstrap, rerun, and reconciliation tests.
- [ ] Add a live source-contract module under `tests/external/` covering the registered identity, the consumed contract, upstream-outage classification, and credential handling; register the source's key in `tests/support/external.py::REQUIRED_SCHEDULED_CREDENTIALS` and in the `external-contract` workflow so a source cannot drop out of live coverage by skipping silently.
- [ ] Declare the source's API discovery entry in `apps/api/registry.py` (`SOURCE_DISCOVERY`) — and a `ServingContract` if it serves the shared observation shape — so `/api/v1/catalog/capabilities` advertises how clients reach it; a source absent from the registry is discoverable through the glossary but reports no routes.
- [ ] Declare the source's neutral observation dispatch entry in `apps/api/registry.py` (`OBSERVATION_DISPATCH`): its latest and as-released serving relations, the metric-identity strategy matching what its `metric_publisher` writes into `physical_lineage`, its release/period/geo/value projections, and its supported neutral filters — so `/api/v1/observations` and `/api/v1/observations/releases` can answer for its metrics. A metric whose source has no dispatch entry is rejected with an explanation, never guessed at.
- [ ] Update the testing catalog, operations documentation, and compatibility/deprecation notes.

Copying the current Census, BLS, or FRED raw/gold layout is not an approved shortcut: those packages contain legacy boundaries tracked by ARCH-002 through ARCH-007.
