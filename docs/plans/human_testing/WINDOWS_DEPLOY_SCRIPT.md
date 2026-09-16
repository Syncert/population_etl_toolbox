# Check: `deploy_stack.ps1` still behaves after its rewrite

## What this checks

`scripts/deploy_stack.ps1` went from **355 lines to 123**. Every rule it used
to carry — which compose and env file each mode uses, which services start,
and the refusal to run `airflow-init` against the warehouse — moved into
`tools/deployment.py`, which the PowerShell script now calls for a JSON plan
and then executes with its own `docker compose` invocation.

Its interface is meant to be **unchanged**: same parameters, same messages,
same exit codes. This confirms that.

## Why it was not automated

There is no Windows runner in CI and no PowerShell in the container the
rewrite was done in. The Python side of the contract is unit-tested and CI
runs the POSIX entrypoint for real, but **the PowerShell script itself has
run nowhere.**

- Filed by: [`docs/plans/completed/A_PORTABLE_DEPLOYMENT_PATH_PLAN.md`](../completed/A_PORTABLE_DEPLOYMENT_PATH_PLAN.md)
- Catalog row: **DEPLOY-008** in [`docs/reference/TESTING_CONTRACT.md`](../../reference/TESTING_CONTRACT.md)

## Heads-up: this script now needs Python

That is a real change. The rewrite put the rules in one place, and the
PowerShell script reads them from there. It resolves `python`, `python3`, or
`py` from `PATH`, and fails with an actionable message if none is found. The
repository already required Python 3.11 of an operator for
`provision_app_api.py` and `provision_api_readonly.py`, so this should not be
new to your machine — but if it is a problem for how you deploy, say so,
because that is a design question worth reopening.

## You need

- Windows with PowerShell
- **Docker Desktop** running
- **Python 3.11** on `PATH`

**Time:** about 15 minutes.
**Touches:** starts local containers; step 5 removes them. Local only.

## Steps

Run every command from the repository root.

**1. Confirm the script finds Python at all.** With no env file present, ask
for external mode — it should refuse for a *missing env file*, which proves
it reached the decision module:

```powershell
./scripts/deploy_stack.ps1 -Mode external -Action up
```

Expect a message naming `infra/docker/stack.external.env` and the
`.example` file to copy from, and a non-zero exit.

**2. Create the internal env file:**

```powershell
Copy-Item infra\docker\stack.env.example infra\docker\stack.env
```

**3. Run the normal lifecycle:**

```powershell
./scripts/deploy_stack.ps1 -Action init
./scripts/deploy_stack.ps1 -Action up
./scripts/deploy_stack.ps1 -Action down
```

Each should log lines prefixed `[deploy:internal/<action>]`, exactly as
before the rewrite.

**4. Make the guard fire.** Edit `infra\docker\stack.env` and set:

```
AIRFLOW_METADATA_DB_HOST=analytics_postgres
AIRFLOW_METADATA_DB_NAME=population_etl
```

Then:

```powershell
./scripts/deploy_stack.ps1 -Action init
```

**This must refuse**, name both databases, and exit non-zero. Read the last
three lines of the refusal carefully — they should suggest
**`-WithLocalAirflow`** and **`-AllowAirflowMetadataInWarehouse`**, in
PowerShell spelling. If they suggest `--with-local-airflow` or
`--allow-airflow-metadata-in-warehouse`, that is a bug: the refusal is giving
you advice you cannot follow.

Then confirm the documented bypass:

```powershell
./scripts/deploy_stack.ps1 -Action init -AllowAirflowMetadataInWarehouse
```

It should announce the bypass and proceed. **Stop it** rather than letting it
migrate, then restore the env file:

```powershell
Copy-Item -Force infra\docker\stack.env.example infra\docker\stack.env
```

**5. Clean up:**

```powershell
./scripts/deploy_stack.ps1 -Action down
```

## What good looks like

- [ ] Step 1 refuses for a missing env file and names the example to copy
- [ ] Step 3's three actions behave as they did before the rewrite, with the
      same `[deploy:internal/<action>]` log prefix
- [ ] **Step 4 refuses**, names both databases, and suggests the
      **PowerShell** flag spellings
- [ ] The escape hatch announces itself
- [ ] Exit codes are 0 on success and non-zero on refusal

## If it fails

Open an issue with:

- which numbered step failed;
- the complete output, including the `[deploy:...]` lines;
- `$PSVersionTable.PSVersion`, `python --version`, `docker version`.

Two failures worth calling out specifically, because they are the ones this
rewrite could plausibly have introduced:

- **"No Python interpreter found on PATH"** — the script cannot reach the
  decision module. Tell me which of `python`, `python3`, `py` your machine
  actually has.
- **The refusal suggests POSIX flags** — the wrong `FlagNames` are being
  passed. That is a one-line fix and worth reporting even though it is
  cosmetic, because a refusal that gives unusable advice is a refusal people
  work around.
