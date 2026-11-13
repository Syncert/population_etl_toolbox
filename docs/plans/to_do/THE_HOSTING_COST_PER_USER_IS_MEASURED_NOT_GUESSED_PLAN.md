---
id: hosting-cost-per-user
branch: claude/hosting-cost-per-user
depends_on: []
parallel_safe: true
complexity: medium
verify:
  - python -m pytest tests/unit/metering -q
  - ruff check .
  - npm --prefix apps/web run test:unit
  - npm --prefix apps/web run lint
---

# The hosting cost per user is measured, not guessed

## Plan status

- **Status:** To do. Drafted 2026-10-14 at Nick's request: "Is there a way to
  implement robust enough compute use logging that you could estimate the cost
  of cloud hosting? … After demoing it enough I'd need an estimate of what
  money it would take per estimated user to host it. For financial estimates."
- **Blocked on:** nothing in code. The measurement period needs the internal
  stack running and being demoed; the Postgres change in deliverable 2 needs
  one warehouse container restart, which waits until no load is running.
- **Current milestone:** none started.

## The answer, and its limits

Yes. Every cost a cloud provider bills for this stack is a function of
quantities the internal stack can record: CPU-seconds and memory per service,
bytes stored, bytes sent to readers, and database work. Logged against how
much the site was used over the same period, those give a fixed monthly cost
plus a cost per active user, priced against a provider's published rates.

Three limits the estimate must state rather than hide:

1. **Demo traffic is not public traffic.** One person demoing hits the same
   places repeatedly, so cache hit rates run high and per-user cost runs low.
   The replay in deliverable 5 adds a broader, scripted mix so the estimate
   does not rest on demo habits alone.
2. **A local core is not a cloud vCPU.** OFFICE_LAB's desktop CPU is faster
   per core than a typical cloud vCPU (which is one hyperthread). The report
   applies a stated conversion range, not a single number.
3. **Clouds bill for provisioned capacity, not used capacity.** A 4-vCPU
   instance costs the same idle or busy. So the realistic estimate comes from
   right-sizing instances to measured peak and capacity (deliverable 6b);
   pure usage pricing (6a) is shown beside it as the floor.

## What drives the bill, and which part scales with users

| Cost | Driver | Scales with users? |
| --- | --- | --- |
| Warehouse storage | GB stored (≈670 GB now, ≈2.7 TB at full ACS history) plus backups | No: fixed, and the largest fixed cost |
| Warehouse compute | Instance size needed for query load and memory | In steps |
| API, web, tile servers | CPU and memory per request served | Yes |
| Redis cache | Memory held | Weakly |
| Egress | Bytes sent to browsers (pages, JSON, map tiles) | Yes |
| Ingestion (Airflow loads) | CPU-hours and disk writes per refresh | No: per data release |
| Backups and WAL | GB written per day | No: follows ingestion |

The estimate keeps fixed and per-user costs apart, because at low user counts
storage alone dominates and the "per user" figure is mostly fixed cost divided
by a small number.

## Deliverables

### 1. A resource sampler for every container

A small Python sampler (`src/data_ingestion_toolbox/metering/`) that reads the
Docker Engine stats endpoint once a minute for every service in the compose
project and appends one JSON line per container per sample: service name,
timestamp, cumulative CPU time (from the cgroup counter, so a missed sample
loses no CPU), memory working set, block bytes read and written, and network
bytes in and out.

- It runs as an opt-in compose service under a `metering` profile, so the
  default `make up` is unchanged. `make metering-up` starts it.
- It needs read access to the Docker socket. That is acceptable for a local
  demo stack and is the reason it is a profile rather than part of any
  deployment; a cloud host reports these numbers itself.
- Samples land under a mounted directory outside the warehouse
  (`infra/docker/metering/`, git-ignored), one file per day. Writing them to
  the warehouse would add load to the very thing being measured.
- The sampler records the host's CPU model, core count and memory once per
  start, so the report can apply the vCPU conversion.

### 2. Database work the containers cannot see

Container CPU says the warehouse was busy; it does not say why. Add
`pg_stat_statements` to the warehouse Postgres
(`shared_preload_libraries` in `infra/docker/docker-compose.yml`, plus the
extension in the warehouse manifest) and have the sampler snapshot, every 15
minutes:

- `pg_stat_database` block reads, hits and transaction counts,
- the top statements by total time (query fingerprint only, never parameter
  values),
- database and per-schema size, and WAL position (WAL generated per day sizes
  backup and replica cost).

The container restart this needs waits until no load is running. Record the
change in `docs/reference/BETA_RESET_REINGESTION.md` if the bootstrap steps
change.

### 3. Bytes served on the request lines

The API completion line (`apps/api/telemetry.py`) already carries route shape,
status, duration and cache disposition. Add response bytes, so egress is known
per route rather than only per container. Martin tile responses are measured
by container network bytes, which is enough. No new identifier, query value or
body goes on the line; the module's existing rule stands.

### 4. A count of users that identifies nobody

"Per user" needs a user count, and this repository keeps identifiers out of
logs on purpose (`apps/api/telemetry.py`, `apps/web/lib/clientReport.ts`).
So users are counted, not tracked:

- The web server's middleware counts page views per route shape and estimates
  daily unique visitors by hashing the client address and user agent with a
  salt that lives only in memory and rotates every day. Only the daily counts
  are written, never the hashes, so no visitor can be followed from one day to
  the next or picked out within a day.
- Sessions are estimated the same way: a gap of 30 minutes without a page view
  starts a new one.
- Signed-in accounts, once self-service accounts ship, are counted per day as
  a number, not by account.
- Nick's own demo traffic is still traffic. Requests from the replay in
  deliverable 5 carry a header that marks them as synthetic so the report can
  show demo, replay and other traffic separately.

### 5. A scripted reader session, for capacity and a broader mix

A replay script (`python -m scripts.replay_reader_sessions`) that walks a
fixed, versioned list of reader journeys against the running stack: the home
page, a state, a county and a city page, a map, a comparison, the workbench.
It runs at a chosen concurrency and records latency per route.

Run at rising concurrency, it answers the sizing question demo use cannot:
how many concurrent readers one API worker, one web worker and the warehouse
serve before p95 latency passes a target (default 1 s for pages, 300 ms for
API reads). Cold-cache and warm-cache runs are reported separately.

### 6. The cost report

`python -m scripts.hosting_cost_report --since <date> --prices <file>` reads
the samples and the daily counts and writes a Markdown report with:

- **Measured use per unit of demand:** CPU-seconds, memory, database block
  reads and egress bytes per page view and per visitor-day, by service, with
  the cache hit rate the period ran at.
- **6a. Usage-priced floor:** measured resource × unit price, by service.
- **6b. Right-sized monthly cost:** the smallest instance shapes that fit the
  measured peak memory and the replay's capacity with 30% headroom, plus
  storage, backups, egress and managed Redis, at user tiers of 100, 1,000,
  10,000 and 100,000 monthly active users. Each tier says which resource
  forced the next instance step.
- **Cost per user** at each tier, split into the fixed share and the variable
  share, with a low and high figure from the vCPU conversion range and the
  demo-versus-replay cache hit rates.
- **What would change the answer:** the assumptions in one list (visits per
  user per month, pages per visit, cache hit rate, vCPU factor, storage size).

Prices live in a dated file (`docs/reference/hosting_prices.yaml`), one entry
per provider tier with its source URL and the date it was read. Prices are
fetched from the providers' own price pages when the plan is implemented, not
written from memory, and the report prints the date so a stale price sheet is
obvious. Start with two shapes: a managed one (managed Postgres, container
service) and a self-managed one (virtual machines with attached SSD), on one
major provider; a second provider is an entry in the file, not new code.

### 7. When the estimate is ready

The report says whether the period behind it is enough, and refuses to print
per-user figures until it is:

- at least 14 days of continuous samples, including at least one data load,
- at least 200 visitor-days of real or demo use, and
- one completed replay at rising concurrency, cold and warm.

## Out of scope

- Choosing a cloud provider or deploying anything. This plan produces the
  number; `remote-warehouse-catches-up` and `deployment-smoke-target` own
  getting a deployment.
- Billing or quota features for users.
- Live dashboards. A Markdown report run on demand is enough for a financial
  estimate; Grafana or Prometheus can come later if a deployment needs them.

## Acceptance criteria

- With the `metering` profile up, one JSON sample per container per minute is
  written, and a unit test proves the CPU figure comes from the cumulative
  counter (a dropped sample does not lose CPU time).
- `pg_stat_statements` is loaded in the warehouse, and the snapshot records
  statement fingerprints with no parameter values (tested on a fixture).
- The API completion line carries response bytes; the existing test that
  forbids query values and identifiers on the line still passes.
- Daily visitor and session counts are written with no hash, address or agent
  stored; a test proves the salt rotates daily and is never written.
- The replay script runs against the stack, marks its traffic as synthetic,
  and reports latency per route at each concurrency step.
- The cost report runs from fixture samples and a fixture price sheet and
  produces every section in deliverable 6, and refuses per-user figures when
  deliverable 7's thresholds are not met.
- `docs/reference/` gains a short metering note (what is recorded, where, and
  what is never recorded), and `docs/plans/EXECUTION_ENVIRONMENTS.md` is
  regenerated.

## Validation

Unit tests cover the sampler's parsing of Docker stats, the counter
arithmetic, the visitor counter's salt rotation, and the report's arithmetic
on fixture data. The live part (the sampler against Docker Desktop on
OFFICE_LAB, the Postgres restart, the replay) runs on the local machine and
its results are recorded here, along with the first real report.

## Checkpoint

Not started.
