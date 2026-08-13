# SQL assets

`martin_seed.sql` is an automated fixture loaded by the disposable Compose
stack. The three `*_silver_diagnostics.sql` files are manual, read-only
operational diagnostics; they contain no authoritative test assertions and are
not credited to the catalog. Automated silver contracts and reconciliation
failures live in `tests/integration/database/` and `tests/e2e/`.
