# Check: a Windows checkout can run the test suites

## What this checks

That the install steps written in `README.md` and
[`docs/user-guides/RUNNING_TESTS.md`](../../user-guides/RUNNING_TESTS.md)
actually work on Windows, and leave you able to run the Python and frontend
suites.

## Why it was not automated

`make bootstrap` is the Linux and macOS surface; Windows uses
`tests\run.ps1`. CI runs on `ubuntu-latest` only, and the container the
instructions were written in is Linux. So the PowerShell block in both
documents was written from the existing documented steps and **never
executed**.

- Filed by: [`docs/plans/completed/A_FRESH_CLONE_CAN_RUN_THE_CHECKS_PLAN.md`](../completed/A_FRESH_CLONE_CAN_RUN_THE_CHECKS_PLAN.md)
- Catalog row: **ENV-020** in [`docs/reference/TESTING_CONTRACT.md`](../../reference/TESTING_CONTRACT.md)

## You need

- A Windows machine with PowerShell
- **Python 3.11** on `PATH` (`python --version`)
- **Node 24** on `PATH` (`node --version`) — `apps/web/package.json` declares
  `engines: node >=24 <25`
- A fresh clone, or an existing one with no `.venv` and no
  `apps\web\node_modules`

**Time:** about 15 minutes, most of it downloads.
**Touches:** creates `.venv\` and `apps\web\node_modules\` in the checkout.
Both are gitignored. Nothing outside the repository, no services, no network
writes.

## Steps

Run every command from the repository root.

**1. Start from a clean checkout.** If `.venv` or `node_modules` already
exist, remove them, or the check proves nothing:

```powershell
Remove-Item -Recurse -Force .venv -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force apps\web\node_modules -ErrorAction SilentlyContinue
```

**2. Create and activate a virtual environment:**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Your prompt should now start with `(.venv)`. If PowerShell refuses to run the
activation script, that is an execution-policy block, not a repository
problem — `Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass` for
this session, then activate again.

**3. Install the Python side:**

```powershell
python -m pip install --upgrade pip
python -m pip install -e ".[local]"
python -m pip check
```

`pip check` should print `No broken requirements found.`

**4. Install the frontend side:**

```powershell
npm ci --prefix apps/web
```

**5. Run the suites:**

```powershell
python -m pytest tests/unit -q
npm --prefix apps/web run test:unit
.\tests\run.ps1 unit
```

## What good looks like

- [ ] Step 3 ends with `No broken requirements found.`
- [ ] Step 4 completes without an `EBADENGINE` **error** (a *warning* about
      Node versions is acceptable; an error is not)
- [ ] `python -m pytest tests/unit -q` ends with **0 failures**. It printed
      `1792 passed` when this was written; that number only grows, so what
      matters is that nothing fails and nothing errors during collection
- [ ] `npm --prefix apps/web run test:unit` ends with **0 failures**
      (`557 passed` at the time of writing)
- [ ] `.\tests\run.ps1 unit` runs the same tier without a separate install

## Known differences from Linux, which are not failures

- **Airflow tiers do not run on Windows.** `dags` and `dag-pipeline` fail
  before reaching an assertion, as does any module importing `airflow` at
  collection time. This is documented in `RUNNING_TESTS.md` under *Native
  Windows* and is expected. Do not report it.
- **`make` is absent.** That is why this document exists; `make bootstrap` is
  not expected to work here.

## If it fails

Open an issue with:

- which numbered step failed;
- the exact command and its complete output;
- `python --version`, `node --version`, `npm --version`, and your Windows
  version.

The most likely real finding is the Node version: CI uses Node 24 and the
package declares `>=24 <25`, but `engine-strict` is not set and there is no
`.npmrc`, so `npm ci` only warns on an older Node. If you are on Node 22 or
23 and the frontend suite misbehaves, say so — that is a genuine gap between
what the package declares and what is enforced.
