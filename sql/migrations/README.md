# Beta schema steps

Apply these checked-in SQL files in numeric order when creating a fresh database. During the beta prototype, rebuilding the database and re-ingesting source history is the default rollback/cutover strategy; production-style compatibility migrations are not required.

Files must remain safe to rerun during bootstrap and must carry the constraints needed by the active design. Do not edit a step after other shared environments depend on it; add the next sequence instead.

Current sequence:

1. `001_raw_capture_control_foundation.sql` creates immutable response capture and control-plane request/run state.
2. `002_gold_glossary_decoupling.sql` creates the provider-neutral publisher registry, harvest state, metric source-fact columns, and durable publisher-ready outbox.
3. `003_semantic_policy_extraction.sql` removes authored policy from the shared metric contract after source report and API consumers have been cut over. It is part of every fresh bootstrap.
4. `004_fred_capture_cutover.sql`, `005_census_acs_capture_cutover.sql`, and `006_bls_capture_cutover.sql` add source-shaped silver revision tables backed by immutable captures.
5. `007_remove_legacy_parsed_raw.sql` removes the beta-era parsed-observation staging relations after the capture-first cutover.
6. `008_geography_reference_cutover.sql` removes ACS geography ownership after the versioned shared-reference schema is installed.
7. `009_census_pep_registry.sql` adds the Census PEP registry and control state.
8. `010_cdc_pipeline.sql` adds the CDC capture, silver, and gold contract.
9. `011_fbi_ucr_pipeline.sql` adds the FBI UCR capture, silver, and gold contract, including `control.fbi_ucr_release`, the `silver_fbi` release/agency/observation model, and the `gold_fbi` publication views.
10. `012_usda_nass_crop_pipeline.sql` adds the USDA NASS Quick Stats capture, silver, and gold crop-data contract.
