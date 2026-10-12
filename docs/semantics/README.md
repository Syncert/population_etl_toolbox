# Analytics definitions

Reviewed business definitions and serving guidance live here, outside warehouse
refresh transactions. Each definition links to a stable harvested `metric_code` and
must record its lifecycle state, owner, reviewer, version, effective date, review
date, intended use, limitations, and source citations.

The glossary harvest never reads this directory. A documentation publishing failure
therefore cannot block raw capture, silver transformation, gold publication, or the
source-derived data API. Personal and team display preferences belong in the
application configuration store, not in this versioned global registry.

Until a reviewed definition exists, consumers display the harvested source label and
an explicit `not reviewed` state. They must not infer an aggregation default.

`time_aggregation_methods.json` (schema `time_aggregation_method.schema.json`) is
the ADR-0007 registry of time-aggregation methods: one entry per served sub-annual
BLS, FRED and FBI UCR metric, each `sum`, `mean`, `end_of_period`,
`recompute_ratio` or `not_aggregable`, with its rationale and citations. Only an
entry with status `approved` authorizes a derived quarter, year or window;
`draft` entries are proposals awaiting the owner's review. Methods are taken from
the provider's own published formulas, never from units: where a provider states
no formula, the entry is `not_aggregable`, and where the provider publishes the
coarser value itself (BLS `M13`), that provider fact is served instead.
