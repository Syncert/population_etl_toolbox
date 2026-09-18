# Running the internal stack

The internal stack is the development target: a local Airflow, warehouse, API,
tile server and web app, ingesting all seven registered sources into
`analytics_postgres`. This page is how to start it and the one mistake that
looks like it worked.

## Start it

```bash
python scripts/deploy_stack.py --mode internal --action up
# or
make deploy-up
```

To see what that resolves to without running anything:

```bash
python scripts/deploy_stack.py --mode internal --emit-plan
```

It prints the compose invocation and a guard verdict as JSON. The invocation is
the important part:

```text
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml up -d
```

## Do not start it with a bare `docker compose up`

**This is the failure mode, and it is silent.**

`docker-compose.yml` passes every credential with an empty default:

```yaml
CENSUS_API_KEY: ${CENSUS_API_KEY:-}
USDA_NASS_API_KEY: ${USDA_NASS_API_KEY:-}
CDC_SOCRATA_APP_TOKEN: ${CDC_SOCRATA_APP_TOKEN:-}
```

Compose auto-loads `infra/docker/.env`, which holds PostgreSQL tuning and **no
credentials**. All six API keys live in `infra/docker/stack.env`, which Compose
does **not** auto-load -- it is passed with `--env-file`, which is why the
deploy script exists.

So `docker compose -f infra/docker/docker-compose.yml up -d` starts a stack
that looks healthy and whose sources run with whatever happens to be exported
in the shell. Every container comes up, the scheduler runs, DAGs are unpaused,
and only the sources whose keys are absent fail -- one source at a time, in
task logs nobody is watching.

It happened. On 2026-09-18 the warehouse showed:

| Source | State | Why |
|---|---|---|
| CENSUS_ACS, BLS, FRED | working | those three keys were also exported in the machine environment |
| USDA_NASS | 48 failures | `missing_api_key`, then HTTP 403 |
| CDC | 8 failures | HTTP 403 on Socrata |
| FBI_UCR | 4 failures | HTTP 404 |

The three that worked were the three whose keys happened to be set on the
machine. The three that failed were the three that only exist in `stack.env`.
Restarting through `deploy_stack.py` fixed all of them on the next scheduled
run, with no code change.

**How to tell, in one command:**

```bash
docker compose --env-file infra/docker/stack.env -f infra/docker/docker-compose.yml \
  exec -T airflow-scheduler printenv USDA_NASS_API_KEY
```

Empty output means the stack was started without `stack.env`. Bring it down and
start it again with the deploy script.

## What `airflow-init` sets up

The deploy runs it before starting the stack, and it is not optional:

- the `public_data` Airflow connection, which every DAG uses to reach the
  warehouse. Without it tasks fail in under a second with
  `The conn_id 'public_data' isn't defined`, before they write an
  `ingestion_run` row -- so the failure is invisible in the warehouse and
  visible only in task logs.
- the API rate-limit pools. `usda_nass_api` has **one** slot on purpose:
  QuickStats 403-throttles above roughly one request per second sustained, and
  the pool is what serialises it.

## Checking that it is actually ingesting

The warehouse is the honest answer, not the Airflow UI:

```sql
SELECT source_code, status, count(*), max(started_at)
  FROM control.ingestion_run
 WHERE started_at > now() - interval '1 day'
 GROUP BY 1, 2 ORDER BY 1, 2;
```

Seven source codes should appear over a full cycle (`CENSUS_GEO` makes eight;
it feeds the shared geography reference). A source with no row has not run; a
source with `failed` rows carries its reason in `error_summary`, which names
the cause directly -- `missing_api_key` is a configuration fault, `HTTP 403`
after a working key usually means throttling, and a check-constraint violation
is a defect worth a ticket.

## The remote warehouse is not this

`192.168.50.16` is a separate, older deployment carrying three of the seven
sources. It is not what anything is developed or verified against, and a
measurement taken there can be confidently wrong about the repository -- see
`docs/plans/to_do/THE_REMOTE_WAREHOUSE_CATCHES_UP_WITH_THE_INTERNAL_STACK_PLAN.md`,
which records both its backlog and an example of exactly that mistake.
