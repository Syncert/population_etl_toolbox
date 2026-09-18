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

- **Status:** Ready for review. All four deliverables are written, every tier
  a cloud session can run is green, and the smoke assertion in deliverable 3
  was run for real against the built API image on a machine session on
  2026-09-18 -- both passing and, with the level at `WARNING`, failing.
- **Last updated:** 2026-09-18
- **Next pickup:** none.

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

- [x] After `create_app()`, `logging.getLogger("apps.api.request")` has an
      effective level of `INFO` or lower and a handler that is not the
      last-resort handler, asserted without `caplog`.
- [x] A request made through the test client under a plain `StreamHandler`
      shows the completion line with its request id -- the id the response
      actually answered with, not a fixture's.
- [x] `API_LOG_LEVEL=WARNING` suppresses the completion line and keeps the
      unhandled-failure record (API-088 stays green).
- [x] The deployment smoke job fails if the completion line is absent from
      the container log, proved failing-first with the level at `WARNING`.
      Run 2026-09-18; see "The machine run". The assertion lives in
      `frontend-smoke.yml`, not `deployment-smoke.yml`; see below.
- [x] `TESTING_CONTRACT.md` gains API-143 and DEPLOY-009;
      `CI_EVIDENCE_MAP.md` names the new files.

## Implementation evidence

### The configuration

`apps/api/logging.py` attaches one named stream handler to `apps.api` -- the
parent of every logger this application writes through -- with a
timestamp/level/logger formatter, and sets that logger's level from
`API_LOG_LEVEL`. `create_app` applies it first, before anything else can log.
Three decisions are worth stating, because each is a thing the plan left open:

- **Uvicorn's loggers are untouched.** Two processes' logging conventions do
  not need to be merged to fix one of them, and the plan offered
  `--log-config` in the Dockerfile as an alternative. Application ownership
  was chosen: it travels with the code, applies to `pytest`, `uvicorn` and
  anything else that imports the app, and needs no edit in three Compose
  files to stay true.
- **Propagation is left on.** With no handler on the root logger nothing is
  duplicated, and the last-resort handler is never reached because a handler
  *was* found. Turning propagation off would have broken every existing
  assertion about this line, which is written through `caplog` -- and
  silently, which is the failure mode this plan exists to end.
- **The request id is not a formatter field.** It is already inside the
  completion line's message. A `%(request_id)s` in the format would print a
  placeholder on every record that does not carry one; giving the other
  records a real id needs a contextvar, and this plan says it does not change
  what the line contains.

The handler is named and replaced rather than added, so building the
application three times still logs each line once -- asserted, because
`create_app` runs once in a container and many times in a test session.

### Why `API_LOG_LEVEL` is validated at settings construction

`int()` is the validation for every other numeric setting here; a level is a
name, and an unrecognised one would otherwise be accepted by `setLevel` as a
custom level number and log nothing. `API_LOG_LEVEL=INFORMATIONAL` now fails
the process at startup with the accepted names. `NOTSET` is deliberately not
accepted: on a logger it means "inherit", which for `apps.api` means the
root's `WARNING` -- the exact silence this plan is about.

### The plan named the wrong workflow

Deliverable 3 asks `deployment-smoke.yml` to assert the line in
`docker compose logs api`. That job's `api` service is an **nginx stub**
(`infra/docker/docker-compose.test.yml`, serving
`tests/fixtures/martin/api_stub.conf`); it never runs the API image and could
never write a completion line. The assertion is in `frontend-smoke.yml`
instead, which is the only job that builds and runs `Dockerfile.api`. It
probes `/health` with an `X-Request-ID` it chose and greps the container's own
log for that id, which is exactly the promise the consumer guide makes.

### The machine run

Run on 2026-09-18 on a Windows 11 machine with a Docker daemon. The stack is
the one `frontend-smoke.yml` composes -- the only job that builds and runs
`Dockerfile.api` -- brought up with `--build` so the assertion saw the real
image rather than a cached one:

```bash
docker compose -f infra/docker/docker-compose.test.yml \
  -f infra/docker/docker-compose.smoke.yml up --detach --wait --build postgres martin api proxy
curl -fsS -H "X-Request-ID: ${probe_id}" http://127.0.0.1:38000/health
docker compose ... logs --no-color api | grep -F api_request | grep -F "request_id=${probe_id}"
```

The line is there, in the container's own log, carrying the id the client
chose:

```text
2026-09-18T14:33:05+0000 INFO apps.api.request api_request method=GET \
  path=/health status=200 duration_ms=1.3 cache=- request_id=smoke-1789741985-499
```

**Failing-first, which is the half no cloud session can fake.** The `api`
service was restarted with `API_LOG_LEVEL=WARNING` through a throwaway
override file, and the same probe repeated. No `api_request` line appears at
all, the grep finds nothing, and the step exits non-zero -- so the assertion
is load-bearing rather than decorative.

That run also confirms, from outside the test suite, the "uvicorn's loggers
are untouched" decision recorded above: at `WARNING` the container still logs
`INFO:     172.20.0.1:40706 - "GET /health HTTP/1.1" 200 OK` from uvicorn's
own access logger, and only the application's line goes quiet. The two
configurations are genuinely independent.

### Commands

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api/test_operational_hardening.py tests/unit/deployment -q` | 107 passed (was 102) |
| `ruff format --check .` / `ruff check .` | clean, 480 files |
| `python -m pytest tests/unit -q` | 1812 passed (was 1807) |

The configuration was verified to be load-bearing: with the
`configure_logging` call removed from `create_app`, the request logger's
effective level reads `WARNING` and the first test fails -- which is precisely
the state a deployed container was in.

Re-run on the machine session of 2026-09-18, on the branch as it now stands:

| Command | Result |
|---|---|
| `python -m pytest tests/unit/api/test_operational_hardening.py tests/unit/deployment -q` | 114 passed |
| `ruff format --check .` / `ruff check .` | clean, 493 files |

The counts are higher than the cloud session's because the branch gained tests
afterwards; none of the difference is this plan's.

## Definition of done

A deployed API writes one structured line per request, with the request id
the client was given, and the smoke job proves it on every push.

## What this plan deliberately does not do

- It does not add a metrics endpoint, tracing, or a JSON logging dependency;
  if JSON output is wanted it is a formatter choice inside the one
  configuration, not a second logging path.
- It does not change what the completion line contains.
- It does not give the `WARNING`/`ERROR` records from the middleware, the
  freshness reader and the dependencies a request id. They now carry a
  timestamp, a level and a logger name, which they did not; correlating them
  to a request needs a contextvar, and that is a change to what those lines
  contain.
