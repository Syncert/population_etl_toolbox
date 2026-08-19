"""Static drift checks for authoritative GitHub Actions evidence ownership."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[3]
MANIFEST = ROOT / "tests/support/ci_evidence_manifest.json"


def _workflow(name: str) -> dict:
    # PyYAML treats the YAML 1.1 word `on` as boolean; normalize that key.
    payload = yaml.safe_load(
        (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
    )
    if True in payload:
        payload["on"] = payload.pop(True)
    return payload


def test_authoritative_ci_jobs_have_stable_names() -> None:
    """Covers: ENV-010 — every protected/release job has a stable identity."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert manifest["version"] == 1
    entries = manifest["required"] + manifest["release"]
    identities = {(item["workflow"], item["job"]) for item in entries}
    assert len(identities) == len(entries)
    for item in entries:
        job = _workflow(item["workflow"])["jobs"][item["job"]]
        assert job["name"] == item["name"]


def test_architecture_paths_trigger_each_owning_workflow() -> None:
    """Covers: ENV-010 — architecture changes cannot bypass owning PR gates."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    for path, owners in manifest["architecture_path_owners"].items():
        for owner in owners:
            trigger = _workflow(owner)["on"]
            pull_request = trigger.get("pull_request", {})
            paths = (
                pull_request.get("paths") if isinstance(pull_request, dict) else None
            )
            target_prefix = path.split("**", 1)[0]
            covered = paths is None or any(
                target_prefix.startswith(pattern.split("**", 1)[0]) for pattern in paths
            )
            assert covered, f"{owner} does not own {path}"
