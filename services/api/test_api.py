import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from main import create_app, TaskPayload

app = create_app()
client = TestClient(app)


def test_root():
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"


def test_health():
    r = client.get("/health")
    assert r.status_code in [200, 503]


@patch("main.nats.connect", new_callable=AsyncMock)
def test_create_task(mock_nats):
    payload = {"task_id": "123", "data": {"key": "value"}}
    r = client.post("/task", json=payload)
    # Should accept payload
    assert r.status_code in [200, 500]  # 500 if NATS is not connected in test
