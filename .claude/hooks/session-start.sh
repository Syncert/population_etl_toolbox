#!/usr/bin/env bash
#
# SessionStart hook: arrive at a checkout that can run the checks it is graded
# by, rather than installing mid-task.
#
# The hook is one call to `make bootstrap` on purpose. The Makefile is this
# repository's task surface, and a second install definition here would be a
# second answer to "what does ready mean" that drifts from the first.
#
# Deliberately no `set -e`, and the exit status is always 0: a session that
# cannot install is still a session that can read the repository, so a failed
# bootstrap is reported into the transcript and the session continues. A
# SessionStart hook that exits non-zero is surfaced as a broken hook, which
# describes the network rather than the repository.

set -uo pipefail

# A local checkout manages its own interpreter. Creating a virtual environment
# and installing into it on every session start is the web container's need,
# not a contributor's, and doing it to whatever environment they happen to
# have active is not this hook's decision to make.
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"

if ! cd "$project_dir"; then
  echo "session-start: cannot enter ${project_dir}; skipped bootstrap."
  exit 0
fi

bootstrap_log="$(mktemp)"
if make bootstrap >"$bootstrap_log" 2>&1; then
  echo "session-start: \`make bootstrap\` completed."
else
  bootstrap_status=$?
  echo "session-start: \`make bootstrap\` FAILED (exit ${bootstrap_status})."
  echo "session-start: dependencies may be absent or stale. Test and lint"
  echo "commands can fail for that reason alone; re-run \`make bootstrap\`"
  echo "before reporting a suite as broken. Last 40 lines:"
  tail -40 "$bootstrap_log"
fi
rm -f "$bootstrap_log"

# Bootstrap installs into `.venv` when no virtual environment is active, which
# is always the case here. Exporting it makes the documented commands -- bare
# `pytest`, `python -m pytest`, `ruff` -- resolve to that environment for the
# rest of the session, so the one bootstrap command needs no activation step
# after it.
if [ -x "${project_dir}/.venv/bin/python" ] && [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  {
    echo "export VIRTUAL_ENV=\"${project_dir}/.venv\""
    echo "export PATH=\"${project_dir}/.venv/bin:\${PATH}\""
  } >>"$CLAUDE_ENV_FILE"
  echo "session-start: .venv is on PATH; \`python\`, \`pytest\` and \`ruff\` resolve to it."
fi

exit 0
