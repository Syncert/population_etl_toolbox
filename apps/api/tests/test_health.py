from fastapi.testclient import TestClient

from apps.api.main import app

client = TestClient(app)


def test_health_route_returns_ok() -> None:
    response = client.get("/health")
    assert response.status_code == 200

    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["service"] == "data-ingestion-toolbox-api"
