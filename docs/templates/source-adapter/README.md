# Source-adapter starter

Copy this starter only after the expansion gate in the [data-layer remediation plan](../../plans/completed/DATA_LAYER_DESIGN_REMEDIATION_TICKETS.md) is open. Read [ADR-0001](../../decisions/0001-data-layer-boundaries.md) and complete the [new-source checklist](../../reference/ADDING_A_DATA_SOURCE.md) before implementation.

Replace `new_source`, `NEW_SOURCE`, endpoint examples, and placeholder ownership with source-specific values. Do not copy the current Census, BLS, or FRED raw/gold DDL: those packages preserve legacy boundaries while they are migrated.

## Expected starter layout

```text
src/data_ingestion_toolbox/new_source/
├── __init__.py
├── config.py                  # start from config.py.template
├── client.py                  # HTTP only; returns response bytes + metadata
├── capture.py                 # commits lossless capture before parsing
├── metadata.py                # source metadata requests/contracts
├── silver_new_source/
│   ├── __init__.py
│   └── transform.py           # capture replay, parsing, typing, validation
└── gold_new_source/
    ├── __init__.py
    └── publisher.py           # provider-neutral glossary export contract

dags/new_source_ingest_dag.py
sql/migrations/{sequence}_new_source_capture_and_control.sql
tests/fixtures/new_source/
tests/unit/new_source/
tests/integration/database/test_new_source_capture_replay.py
tests/external/test_new_source_contract.py
```

Shared raw-capture and control-plane objects should come from ARCH-004 rather than being reimplemented inside the adapter. Source migrations add only the source-specific objects or registrations needed by that shared contract.

## Required configuration work

1. Copy `config.py.template` to `src/data_ingestion_toolbox/new_source/config.py` and rename the class and constants.
2. Choose one explicit API-key environment variable, normally `NEW_SOURCE_API_KEY`. Document whether it is always required, optional, or required only above an anonymous rate limit.
3. Add an empty placeholder to each relevant tracked runtime example, such as `infra/docker/stack.external.env.example`. Never add the real value.
4. Configure the real secret through the deployment secret store, Airflow connection/secret backend, or an ignored local `.env` file.
5. Validate the key when an HTTP task executes, not at module import, so DAG parsing and offline replay do not require credentials.
6. Ensure request fingerprints, captured headers, exception text, and logs exclude the key and other authorization material.

## First implementation milestones

- [ ] Configuration imports without network, database, Airflow-variable, or secret-store access.
- [ ] A fixture response is stored byte-for-byte or with tested canonical logical equivalence.
- [ ] Capture commit completes before parser invocation.
- [ ] The same fixture replays to silver with network access disabled.
- [ ] A malformed fixture remains captured and produces a sanitized quarantine record.
- [ ] A changed response for the same request retains both checksums/retrieval events.
- [ ] Gold publishes deterministic source facts without dashboard or governance policy.
- [ ] The glossary publisher contract exposes stable keys, source labels, units/grains, lineage, schema version, and watermark.
- [ ] The DAG emits publisher-ready state without waiting for glossary harvest.
- [ ] Fresh bootstrap, rerun, validation, and full reset/re-ingestion behavior are tested.

## Agent handoff block

Keep this block in the source's implementation plan while work is active:

```markdown
### Implementation checkpoint

- Last updated: YYYY-MM-DD
- Current milestone: configuration | capture | replay | silver | gold publisher | DAG | cutover
- Last passing command: `<exact command>`
- Completed: ...
- Next action: ...
- Known limitation/blocker: ...
- Migration applied only to disposable test database: yes/no
```
