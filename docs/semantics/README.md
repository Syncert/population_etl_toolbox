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
