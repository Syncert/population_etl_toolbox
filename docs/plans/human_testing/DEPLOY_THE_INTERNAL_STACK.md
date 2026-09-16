# Check: the deployment entrypoint brings the real stack up, and refuses the dangerous case

## What this checks

Two things, and the second is the important one.

1. `make deploy-init` / `deploy-up` / `deploy-down` start and stop the **real
   internal stack** — `infra/docker/docker-compose.yml`, including Airflow.
2. The entrypoint **refuses to run `airflow-init` when Airflow's metadata
   database and the warehouse are the same database**, live, against a real
   env file.

## Why it was not automated

CI drives the *disposable* stack (`docker-compose.test.yml`), which has no
`airflow-init` service — so the refusal has never fired anywhere except in
unit tests and two hand-run commands. The container the work was written in
has the Docker CLI but no reachable daemon.

What CI does already cover, so you do not need to: the entrypoint starting
and stopping containers, and a failed Compose call propagating a non-zero
exit. Those run on every push in `deployment-smoke`.

- Filed by: [`docs/plans/completed/A_PORTABLE_DEPLOYMENT_PATH_PLAN.md`](../completed/A_PORTABLE_DEPLOYMENT_PATH_PLAN.md)
- Catalog row: **DEPLOY-008** in [`docs/reference/TESTING_CONTRACT.md`](../../reference/TESTING_CONTRACT.md)

## You need

- Linux or macOS with **Docker** and **Docker Compose** running
  (`docker info` must succeed)
- Python 3.11
- Several GB of free disk — this pulls Postgres, Redis, Martin, Airflow and
  the web image

**Time:** about 20 minutes, mostly image pulls the first time.
**Touches:** starts real containers and local Docker volumes on your machine.
Step 6 removes them. This is a local stack; it is not production and it
reaches no deployed host.

## Steps

Run every command from the repository root.

**1. Create the env file:**

```bash
cp infra/docker/stack.env.example infra/docker/stack.env
```

`infra/docker/stack.env` is gitignored. You do not need to edit it for this
check.

**2. See what would run, without running it:**

```bash
make deploy-plan
```

This executes nothing. It prints JSON: the compose file, the env file, the
guard's verdict, and the exact `docker compose` commands. The guard should
read `"status": "ok"`.

**3. Initialise, then start:**

```bash
make deploy-init
make deploy-up
```

**4. Confirm the stack is actually up:**

```bash
docker compose --env-file infra/docker/stack.env \
  -f infra/docker/docker-compose.yml ps
```

**5. Now the part only you can do — make the guard fire.**

Open `infra/docker/stack.env` and point Airflow's metadata database at the
warehouse. Set these two values:

```
AIRFLOW_METADATA_DB_HOST=analytics_postgres
AIRFLOW_METADATA_DB_NAME=population_etl
```

Then:

```bash
make deploy-init
```

**This must refuse.** It should print a message beginning *"Refusing to run
airflow-init: the Airflow metadata database and the warehouse are the same
database"*, name both targets as `analytics_postgres:5432/population_etl`,
and exit non-zero. **No container should start.**

Then check the escape hatch still works, deliberately:

```bash
make deploy-init DEPLOY_ARGS=--allow-airflow-metadata-in-warehouse
```

That should announce the bypass and proceed. **Stop it** — do not let it
finish migrating, and undo your edit to `stack.env` afterwards:

```bash
git checkout -- infra/docker/stack.env 2>/dev/null || cp infra/docker/stack.env.example infra/docker/stack.env
```

(`stack.env` is gitignored, so the `cp` is the one that will actually work.)

**6. Tear down:**

```bash
make deploy-down
docker compose --env-file infra/docker/stack.env \
  -f infra/docker/docker-compose.yml ps
```

The final `ps` should list nothing.

## What good looks like

- [ ] Step 2 prints JSON with `"status": "ok"` and runs nothing
- [ ] Step 3 completes and step 4 lists running containers
- [ ] **Step 5 refuses**, names both databases, exits non-zero, and starts
      nothing
- [ ] The escape hatch announces itself rather than bypassing silently
- [ ] Step 6 leaves no containers

## If it fails

The refusal in step 5 is the one that matters. If it does **not** refuse —
if `airflow-init` starts with metadata pointed at the warehouse — that is a
serious finding, because that command writes Airflow's schema into the
warehouse and resets the `public_data` connection and every API pool. Stop
immediately, do not let it complete, and report it.

Open an issue with:

- which numbered step failed;
- the complete output of the failing command;
- your `infra/docker/stack.env` **with passwords removed**;
- `docker --version` and `docker compose version`.
