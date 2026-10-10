# ACS warehouse reload failures, 2026-10-09 to 2026-10-10

Status: root cause fixed in code on branch `claude/project-thread-ypfqia`;
the ACS rebuild was restarted with the fix at 14:33 UTC on 2026-10-10.
All times are UTC.

## Summary

The ACS silver rebuild (`acs_ingest.transform_to_silver`) died seven times in
about 36 hours. Two of those deaths had outside causes: a deliberate Docker
restart, and a Windows Update reboot. The other five came from one bug.

**Root cause.** The transform read each year whole, as one Python list of row
tuples. Years 2005 to 2023 hold 2 to 17 million revision rows each. Year 2024
holds **189,840,286**, because it is the first year with the new place (162M
rows) and tract (10.8M rows) grains. Every attempt that reached 2024 went
silent for about 25 minutes while the scheduler container grew. Then the WSL
VM hit its 96 GB cap, Docker froze, and Airflow killed the task for missing
its heartbeat. Lowering the Postgres memory settings on 10-10 could not have
fixed this. The query result alone was larger than the VM.

**Was it a crash or a restart?** Both happened, but neither was the root
cause.

- At 08:40 Windows Update restarted the PC (`MoUsoCoreWorker.exe`, "Operating
  System: Service pack (Planned)", event 1074). That killed the 07:08 run while
  it was healthily re-checking 2017. It also ended the previous Claude session.
- At 14:12 the PC restarted again, started from Explorer by the logged-in user
  ("Other (Unplanned)").
- On 10-09 the PC also went into idle sleep from 10:29 to 12:32, with the load
  running. That run survived the sleep, but it stalled for two hours.
- No unexpected shutdown happened (no Kernel-Power 41 or event 6008), and
  there was no bugcheck.

## Every attempt

| # | Run / try | Started | Died | Where it was | How it died | Why |
|---|---|---|---|---|---|---|
| 1 | `demo_full_141921` try 1 | 10-09 02:23 | 10-09 13:39 | 2005–2019 written; starting 2020 | `could not translate host name "service_postgres"`, then `conn_id public_data isn't defined` | Docker DNS stopped answering under load. The PC was also asleep from 10:29 to 12:32 mid-run (a 2-hour gap in the log) |
| 2 | `demo_full_141921` try 2 | 10-09 13:59 | 10-09 22:52 | 2005–2019 re-checked, 2020–2023 written (31.7M new facts); reading 2024 | Heartbeat failures from 22:44, then "Heartbeat time limit exceeded", SIGKILL | **2024 read whole**: the VM filled and Docker froze |
| 3 | `demo_full_141921` try 3 | 10-09 23:14 | 10-10 01:59 | 2005–2023 re-checked (no changes); reading 2024 | Heartbeat time limit exceeded | **2024 read whole** (same) |
| 4 | `scheduled__2026-09-01` try 1 | 10-10 02:01 | 10-10 02:51 | re-checking early years | SIGTERM | Deliberate: Docker Desktop restart Nick approved (VM at 95 GB) |
| 5 | `scheduled__2026-09-01` try 2 | 10-10 03:11 | 10-10 06:29 | 2005–2023 re-checked by 05:49; reading 2024 | `server closed the connection unexpectedly`, heartbeat exceeded; Docker reported an OOM kill at 06:30 | **2024 read whole** (same) |
| 6 | `scheduled__2026-09-01` try 3 | 10-10 07:08 | 10-10 08:40 | re-checked 2005–2017, healthy, no heartbeat errors | Container exit 255; DAG file read `Input/output error` at 08:40:28 | Windows Update reboot. It would have reached 2024 at about 08:55 and died there, because Postgres memory was never the problem |
| 7 | `scheduled__2026-09-01` try 4 | 10-10 14:33 | running | new code: big years read in slices | — | — |

Evidence:

- Task logs are in the `docker_airflow_logs` volume under
  `dag_id=acs_ingest/run_id=*/task_id=transform_to_silver/attempt=*.log`. Raw
  row counts by year are on each log's `PRE-TRANSFORM` line.
- Windows System log: events 42 and 107 (sleep and resume), 1074 (restart
  initiator), 6006 and 6005 (event log stop and start).
- `docker inspect`: every container exited 255 at 08:43 (Docker coming back
  after the reboot). `docker-airflow-scheduler-1` has `OOMKilled=true`. All
  core containers have restart policy `no`, so nothing came back on its own
  after either reboot.
- `com.docker.backend.exe.log` 06:30:03: `POST /analytics/track/oom-kills`.
- `silver_census.observation_revision`, year 2024 by level: place 162,051,014;
  county 16,569,828; tract 10,758,132; state 452,608; us 8,704.

## What was misread along the way

- **"Docker DNS failures"** (attempts 1 and 2) were a symptom. The VM was
  starved and stopped answering, DNS included. Pinning hostnames in
  `/etc/hosts` only removed the first thing to fail. It is not re-applied
  after this reboot, because the container IPs changed (service_postgres moved
  from .3 to .2) and a stale pin would break the scheduler.
- **Postgres settings** (shared_buffers 48 GB, 512 MB work_mem) did make the VM
  tighter, and the lowered values (20 GB, 128 MB, 8 workers) are still in
  place. But the 2024 read alone exceeded any setting.
- **"Re-checking finished years"** cost about 1.7 to 2.5 hours per attempt,
  because the transform had no memory of what it had already done.

## Fixes

### In the repository (this branch)

1. **A large year is read in slices.** A year above 20M revision rows is read
   one slice at a time, a slice being one (dataset, geo level, state). Every
   natural key and E/M pair lies inside one slice, so the facts are identical.
   A test proves the sliced and whole-year facts match. The largest 2024 slice
   is one state's places, roughly the size of a whole 2023 year, which has
   always fit.
2. **Rows stream into typed frames.** A server-side cursor pulls 500k rows at
   a time into polars, so a slice never exists as a list of Python tuples.
3. **Resume where it stopped.** The DAG passes its Airflow `run_id` to the
   transform. Each finished slice is recorded in the new table
   `silver_census.transform_checkpoint`. A retry or a cleared task of the same
   run skips the recorded slices and redoes only the one it was on. A new run
   (new key) still replays every year, so Census errata still reach silver.
   This is catalog row ETL-082.

Validation:

- Integration tests against a fresh test warehouse:
  `tests/integration/database/test_census_acs_transform_slices.py` (new), plus
  the existing ACS place, tract and silver-flow tests. 10 passed.
- Unit tier: 2,572 passed. `tests/unit/nces_ccd/test_nces_ccd_adapter.py`
  could not be collected because the host venv lacks `inflate64`. That is an
  environment gap, not related to this change.
- DAG tier in the scheduler container: 293 passed, 5 skipped (DB-backed).
- `ruff check .` is clean. The schema snapshot and the quality inventory are
  regenerated.

### On the machine (Nick's decision; not changed)

1. **Windows Update.** Set active hours, or pause updates, while a multi-hour
   load runs. Today's 08:40 reboot was automatic.
2. **Sleep.** The PC idle-sleeps (event 42, "System Idle"). Set sleep to
   *Never* on AC power for this machine. Claude sessions now ask the app to
   keep the PC awake while they work, but that ends when the session goes idle.
3. **Container restart policy.** The core stack containers have
   `restart: no`, so after a reboot the site, API and Airflow stay down until
   someone starts them. `restart: unless-stopped` on the long-lived services
   would bring them back. This is a repo compose change; it is proposed, not
   made.
4. **`.wslconfig`.** The 96 GB cap is reasonable for a 128 GB host. With the
   slice fix there is no need to raise it.

## Restarting the rebuild

At 14:33 the `scheduled__2026-09-01` run's `transform_to_silver` was resumed on
the new code (try 4). This first attempt under the run key has no checkpoints
yet, so it re-checks 2005–2023 once (about 1.7 hours). Any later retry of this
run starts at the slice it stopped on. Seeding checkpoints for the
already-verified years was considered and not done without Nick's OK.
