# `npm audit --json` fixtures

Reduced to the shape `npm audit --json` emits (`auditReportVersion: 2`) for
`tests/frontend/unit/check-audit.test.js`. The advisory ids and URLs are
placeholders, not real advisories. `override-pinned.json` models the
2026-10-06 failure: `sharp` pinned in `overrides`, so `npm audit fix` cannot
move it, beside a transitive package that `npm audit fix` can, and a moderate
advisory below the gate's level.
