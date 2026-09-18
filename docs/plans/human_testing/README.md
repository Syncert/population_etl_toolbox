# Human testing

Checks and one-off actions that only a person can perform — because they need
a machine, an operating system, or a credential that neither the authoring
container nor CI has.

This is a homework pile, not a workflow state.

## What this folder is not

**It is not part of `to_do/ -> in_progress/ -> needs_review/ -> completed/`.**
The dispatcher does not read it: every file here is written without YAML
frontmatter, so `parse_plan` skips it as "readable guidance rather than
dispatchable work". Nothing here appears in `plan_dispatcher inventory`, and
nothing here blocks a dispatch.

**It does not gate a plan.** A plan can be delivered, accepted, and merged
with items filed here. That is the point: an item lands here precisely so it
stops blocking the next plan.

**It is not an excuse for missing evidence.** Filing something here never
substitutes for an acceptance criterion the plan declared. Everything in this
folder is *residual* verification — either no criterion required it, or the
criterion was satisfied another way and this is the wider check nobody ran.
The completion gate in [`docs/plans/README.md`](../README.md) is unchanged: an
unavailable environment is still not passing evidence, and a criterion that
needs evidence still keeps its plan in `in_progress/`.

## How to use it

Pick any file. They are independent and can be done in any order. Each one
states what it checks, why it was not automated, what you need, roughly how
long it takes, and what it touches.

**When a check passes:** move the file to `completed/` in this folder, or
delete it — whichever you prefer. Either way, note the date and what you ran.

**When a check fails:** that is a finding, not a failure of the merged work.
Open an issue (or tell the agent) with:

- the file you were following;
- the exact command you ran;
- what you expected and what you got, verbatim;
- your OS, Python version, and Docker version where relevant.

A failure here is expected to reopen work. It does not retroactively
un-accept the plan that filed it.

## Current homework

| File | What it checks | Needs | Time |
|---|---|---|---|
| [`WINDOWS_BOOTSTRAP.md`](WINDOWS_BOOTSTRAP.md) | The documented Windows install produces a checkout that can run the suites | Windows, Python 3.11, Node 24 | ~15 min |
| [`DEPLOY_THE_INTERNAL_STACK.md`](DEPLOY_THE_INTERNAL_STACK.md) | `make deploy-*` brings the real internal stack up and down, and the metadata/warehouse refusal fires for real | Linux or macOS with Docker | ~20 min |
| [`WINDOWS_DEPLOY_SCRIPT.md`](WINDOWS_DEPLOY_SCRIPT.md) | `deploy_stack.ps1` still behaves after being rewritten from 355 lines to 123 | Windows, Docker Desktop, Python | ~15 min |
| [`RECORD_THE_IDENTITY_GATE_DECISION.md`](RECORD_THE_IDENTITY_GATE_DECISION.md) | Records an approval you already gave, where the dispatcher can see it | The machine your dispatcher runs on | ~2 min |
| [`ORCHESTRATED_DAG_RUNS_ON_A_CLEAN_AIRFLOW.md`](ORCHESTRATED_DAG_RUNS_ON_A_CLEAN_AIRFLOW.md) | The three real-`DagRun` tests, which need an Airflow metadata database the Windows machine's install cannot create | Any host, Python 3.11, Docker | ~10 min |

`RECORD_THE_IDENTITY_GATE_DECISION.md` is an action rather than a test, and it
is here for the same reason as the rest: it can only happen on your machine.
