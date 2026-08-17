# Adding a data source

Do not onboard another production source until the expansion gate in the [data-layer remediation plan](../plans/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md) is satisfied. When the gate opens, every adapter must follow [ADR-0001](../decisions/0001-data-layer-boundaries.md).

Begin with the [source-adapter starter](../templates/source-adapter/README.md), which provides the expected package layout, a `config.py` template, initial milestones, and an agent handoff block.

## Adapter checklist

- [ ] Register a stable, provider-neutral source identity without editing a closed provider enumeration.
- [ ] Create `src/data_ingestion_toolbox/<source>/config.py` from the starter and define source scope, endpoint, timeouts, concurrency, Airflow pool, and PostgreSQL connection ID without performing I/O at import.
- [ ] Name and document the API-key environment variable; add only an empty placeholder to tracked environment examples and put the real value in an ignored local environment or deployment secret store.
- [ ] Validate credentials when the request executes, not during module/DAG import, and prove secrets cannot enter payload captures, request fingerprints, logs, or exception messages.
- [ ] Define the endpoint, request fingerprint, media type, schema/version identifier, and sensitive-data handling.
- [ ] Persist and commit an append-only, lossless response capture before parsing; include checksum, retrieval time, HTTP metadata, and run lineage.
- [ ] Put attempts, slices, retries, watermarks, errors, and quarantine status in the control plane, not the raw schema.
- [ ] Implement offline raw-to-silver replay from a checked-in representative fixture.
- [ ] Perform parsing, typing, null interpretation, reshaping, deduplication/revision selection, validation, and derived identifiers in silver.
- [ ] Publish only deterministic, data-derived facts in gold.
- [ ] Expose the versioned glossary publisher contract without creating, altering, dropping, or seeding `gold_glossary` objects.
- [ ] Keep definitions, approvals, dashboard defaults, ownership commentary, aliases, and user/team preferences outside source ETL and gold data-product tables.
- [ ] Add ordered forward migration SQL, safe rollback or an explicit no-rollback rationale, grants, validation queries, and migration-state recording.
- [ ] Add unit, contract, integration, replay, malformed-payload/quarantine, bootstrap, rerun, and reconciliation tests.
- [ ] Update the testing catalog, operations documentation, and compatibility/deprecation notes.

Copying the current Census, BLS, or FRED raw/gold layout is not an approved shortcut: those packages contain legacy boundaries tracked by ARCH-002 through ARCH-007.
