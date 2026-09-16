---
id: api-request-log-is-emitted
branch: claude/api-request-log-is-emitted
depends_on: []
parallel_safe: true
complexity: low
verify:
  - python -m pytest tests/unit/api/test_operational_hardening.py tests/unit/deployment -q
  - ruff format --check . ; ruff check .
---

# The request completion line reaches the process log

## Plan status

- **Status:** Unclaimed. Authored 2026-09-16 from the codebase audit; no
  implementation has started.
- **Last updated:** 2026-09-16
- **Current milestone:** not started.

## Why

The API's one request-level operational signal is the completion line
`apps/api/telemetry.py` writes through `logging.getLogger("apps.api.request")`
at `INFO`: method, path, status, latency, cache disposition and the
`X-Request-ID` the guide promises a client can quote "with the server's logs"
(`docs/reference/API_CONSUMER_GUIDE.md`, request-id section). There is no
metrics endpoint and no tracing in `apps/api`, so this line is the whole of
request observability.

Nothing configures logging for the API process. `dictConfig`, `basicConfig`,
`log_config` and `LOG_LEVEL` appear under `apps/`, `src/`, `infra/` and
`.github/` only in `scripts/diagnose_geo_missing.py`. The container runs
`uvicorn apps.api.main:app --host 0.0.0.0 --port 8000 ...`
(`infra/docker/Dockerfile.api:18`, `infra/docker/docker-compose.yml:78`) with
no `--log-config` or `--log-level`. Uvicorn's default configuration attaches
handlers to its own `uvicorn`, `uvicorn.error` and `uvicorn.access` loggers
only; the Python root logger stays at `WARNING` with the last-resort stderr
handler. Every `INFO` record from `apps.api.request` is therefore dropped in
a deployed process, and the `WARNING`/`ERROR` records from
`apps/api/middleware.py`, `freshness.py` and `dependencies.py` reach stderr
with no formatter, no timestamp and no request id.

The unit tests do not see this because they use
`caplog.at_level(logging.INFO, logger="apps.api.request")`
(`tests/unit/api/test_operational_hardening.py`), which installs its own
handler. `.github/workflows/deployment-smoke.yml` uploads `docker compose
logs` as an artifact and asserts nothing about its content. The API-088
unhandled-failure traceback is logged at `ERROR` and survives, but the
id-to-request mapping for every request that did not fail does not.

## Deliverables

### 1. One logging configuration, owned by the application

`apps/api/logging.py` (or a checked-in `log_config.json` beside the app)
that sets `apps.api` and `apps.api.request` to `INFO`, attaches one stream
handler with a formatter that carries timestamp, level, logger and
`request_id` when present, and leaves uvicorn's own loggers as they are.
`create_app` applies it once; a `--log-config` flag in the Dockerfile and both
Compose files is acceptable instead if the reviewer prefers process-level
ownership, but not both.

### 2. The level is a setting

`API_LOG_LEVEL` in `src/data_ingestion_toolbox/config.py`, defaulting to
`INFO`, validated at import like the other integer/enum settings, and named
in `infra/docker/.env.example` and `stack.env.example`.

### 3. The deployment smoke reads its own artifact

`deployment-smoke.yml` asserts that `docker compose logs api` contains one
completion line for the readiness probe it just made, with a request id.

### 4. The guide names the format

The consumer guide's request-id section states what a server log line looks
like, so an operator correlating a client report knows what to grep.

## Acceptance criteria

- [ ] After `create_app()`, `logging.getLogger("apps.api.request")` has an
      effective level of `INFO` or lower and a handler that is not the
      last-resort handler, asserted without `caplog`.
- [ ] A request made through the test client under a plain `StreamHandler`
      captured on stderr shows the completion line with its request id.
- [ ] `API_LOG_LEVEL=WARNING` suppresses the completion line and keeps the
      unhandled-failure traceback (API-088 stays green).
- [ ] The deployment smoke job fails if the completion line is absent from
      the container log (prove failing-first by running it once with the
      level at `WARNING`).
- [ ] `TESTING_CONTRACT.md` gains an `API-` row and a `DEPLOY-` row;
      `CI_EVIDENCE_MAP.md` names the new files.

## Definition of done

A deployed API writes one structured line per request, with the request id
the client was given, and the smoke job proves it on every push.

## What this plan deliberately does not do

- It does not add a metrics endpoint, tracing, or a JSON logging dependency;
  if JSON output is wanted it is a formatter choice inside the one
  configuration, not a second logging path.
- It does not change what the completion line contains.
