"""Static production-container and proxy deployment contracts."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.deployment]

ROOT = Path(__file__).resolve().parents[3]


def _compose(name: str) -> dict:
    return yaml.safe_load((ROOT / "infra/docker" / name).read_text(encoding="utf-8"))


def test_production_images_are_immutable_and_application_images_are_non_root() -> None:
    """Covers: DEPLOY-004 — runtime bases and explicit images use immutable digests."""
    for relative in (
        "infra/docker/Dockerfile.api",
        "infra/docker/Dockerfile.web",
        "infra/airflow/Dockerfile",
    ):
        source = (ROOT / relative).read_text(encoding="utf-8")
        from_lines = [line for line in source.splitlines() if line.startswith("FROM ")]
        assert from_lines and all("@sha256:" in line for line in from_lines)

    for compose_name in (
        "docker-compose.yml",
        "docker-compose.external.yml",
        "docker-compose.airflow.yml",
        "docker-compose.test.yml",
    ):
        services = _compose(compose_name)["services"]
        for service_name, service in services.items():
            if "image" in service:
                assert "@sha256:" in service["image"], (
                    f"{compose_name}:{service_name} has a mutable image"
                )

    assert "USER api" in (ROOT / "infra/docker/Dockerfile.api").read_text(
        encoding="utf-8"
    )
    assert "USER nextjs" in (ROOT / "infra/docker/Dockerfile.web").read_text(
        encoding="utf-8"
    )
    airflow = (ROOT / "infra/airflow/Dockerfile").read_text(encoding="utf-8")
    assert [line for line in airflow.splitlines() if line.startswith("USER ")][
        -1
    ] == "USER airflow"


def test_runtime_hardening_and_published_ports_are_bounded() -> None:
    """Covers: DEPLOY-005 — application runtimes are read-only and ports are bounded."""
    for compose_name in ("docker-compose.yml", "docker-compose.external.yml"):
        services = _compose(compose_name)["services"]
        for service_name in ("api", "martin", "web"):
            service = services[service_name]
            assert service["read_only"] is True
            assert "no-new-privileges:true" in service["security_opt"]

    for compose_name in (
        "docker-compose.yml",
        "docker-compose.external.yml",
        "docker-compose.airflow.yml",
        "docker-compose.test.yml",
    ):
        for service_name, service in _compose(compose_name)["services"].items():
            for published in service.get("ports", []):
                rendered = str(published)
                assert rendered.startswith("127.0.0.1:") or rendered.startswith(
                    "${WEB_BIND_ADDRESS:-127.0.0.1}:"
                ), f"{compose_name}:{service_name} publishes an unbounded port"


def test_next_and_nginx_proxy_only_expected_api_and_tile_origins() -> None:
    """Covers: DEPLOY-001 — public proxies route API/Martin without leaking origins."""
    next_config = (ROOT / "apps/web/next.config.mjs").read_text(encoding="utf-8")
    nginx = (ROOT / "infra/web/nginx.conf").read_text(encoding="utf-8")

    assert 'source: "/api/:path*"' in next_config
    assert 'source: "/tiles/:path*"' in next_config
    assert "destination: `${apiOrigin}/api/:path*`" in next_config
    assert "destination: `${tilesOrigin}/:path*`" in next_config
    assert re.search(
        r"location /api/\s*\{[^}]*proxy_pass http://api:8000;", nginx, re.S
    )
    assert re.search(
        r"location /tiles/\s*\{[^}]*proxy_pass http://martin:3000/;", nginx, re.S
    )
    assert "proxy_set_header Host $host;" in nginx
