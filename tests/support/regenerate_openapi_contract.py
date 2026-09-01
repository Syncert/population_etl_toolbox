"""Regenerate the reviewed public-API contract snapshot.

Run deliberately, as part of a change that intends to move the contract:

    python -m tests.support.regenerate_openapi_contract

Then read the resulting diff. The snapshot is review evidence for what consumers
may depend on; regenerating it to silence a failing test discards the only signal
that a breaking change happened.
"""

from __future__ import annotations

import json
from pathlib import Path

from apps.api.main import app
from tests.support.openapi_contract import contract_digest

SNAPSHOT_PATH = (
    Path(__file__).resolve().parents[1] / "fixtures" / "api" / "openapi_contract.json"
)


def main() -> None:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    digest = contract_digest(app.openapi())
    SNAPSHOT_PATH.write_text(
        json.dumps(digest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(
        f"wrote {SNAPSHOT_PATH} "
        f"({len(digest['operations'])} operations, {len(digest['schemas'])} schemas)"
    )


if __name__ == "__main__":
    main()
