# Scrubbing provider keys from Airflow logs

## What happened

Until log redaction was installed, `httpx` logged every request at INFO as
`HTTP Request: GET <full url>`. Providers that authenticate with a query
parameter therefore had their key written in plain text to the Airflow task
logs in the `docker_airflow_logs` volume (`/opt/airflow/logs` in the
containers):

| Key | Parameter | DAGs that logged it |
| --- | --- | --- |
| `CENSUS_API_KEY` | `key=` | `acs_ingest`, `census_saipe_sahie_ingest`, `census_govfin_ingest`, `elections_ingest` (house county share), `pep_ingest` |
| `EIA_API_KEY` | `api_key=` | `eia_retail_gasoline_ingest` |
| `USDA_NASS_API_KEY` | `key=` | `usda_nass_crop_ingest` |
| `FRED_API_KEY` | `api_key=` | none (FRED already quieted `httpx`) |

`BLS_API_KEY` travels in a POST body and the Dataverse guestbook answers in a
POST body, so neither appeared in a request line.

## What prevents it now

* `data_ingestion_toolbox.log_redaction` holds `httpx`/`httpcore` at WARNING
  and installs a log-record factory that masks secret URL/JSON parameters
  (`key`, `api_key`, `registrationkey`, `$$app_token`, signed-URL signatures …)
  and the literal values of credential environment variables in every
  message and traceback. It is installed on package import, so every DAG and
  script gets it.
* `AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS` in
  `infra/airflow/airflow.env.example` selects
  `data_ingestion_toolbox.airflow_logging.LOGGING_CONFIG`: Airflow's default
  configuration with the same mask on every handler and `httpx` at WARNING.
  It takes effect when the Airflow containers are recreated (`make deploy-up`
  while ingest is idle).
* `sanitize_error_message` (ledger error text) masks the same parameters.

## Scrub the existing logs

Run this in PowerShell on OFFICE_LAB once the fix is deployed, preferably
while no load is running. It runs inside the scheduler container, where the
log volume is mounted and the keys are in the environment, so both `key=…`
values and any bare copy of a key value become `REDACTED`. It prints only
paths and counts, never a key. Files touched in the last two minutes (a
running task's log) are left alone; run it again later to catch them.

```powershell
cd H:\population_etl_toolbox\infra\docker; docker compose exec -T airflow-scheduler python /opt/data_ingestion_toolbox/scripts/scrub_airflow_log_secrets.py /opt/airflow/logs --apply
```

Leave off `--apply` for a report of what would change. Running it again
without `--apply` afterwards should end `would rewrite 0`.

Copies outside the volume are not reached by the scrub: `H:\backfill-logs\`,
anything pasted into chat or project files, and warehouse backups that
include the Airflow metadata database. Search those for `key=` by hand.

## Rotate the keys

A scrub removes the copies we know of; rotation makes any copy useless.
Rotate the Census, EIA and USDA NASS keys (all were written to logs).
Rotating FRED and BLS is optional.

1. Request a new key:
   * Census: <https://api.census.gov/data/key_signup.html> (a new key is
     emailed; activate it from the email link).
   * EIA: <https://www.eia.gov/opendata/register.php> (a new key is emailed).
   * USDA NASS: <https://quickstats.nass.usda.gov/api> (request a new key).
   * FRED, if wanted: <https://fredaccount.stlouisfed.org/apikeys> (create a
     new key, then delete the old one there).
2. Put the new value in `infra/docker/stack.env` on OFFICE_LAB only. Never in
   the repository, chat, project files or memory.
3. While ingest is idle, recreate the stack so the containers read it:
   `cd H:\population_etl_toolbox; make deploy-up`.
4. Trigger one small run of `eia_retail_gasoline_ingest` and of a Census DAG.
   The run should succeed with the new key, and its task log should hold no
   `HTTP Request:` lines.
5. Census, EIA and NASS do not offer a self-service revoke; once the new key
   works, the old one stops mattering as long as no copy is used. Ask the
   provider to deactivate it if you want it dead for certain.
