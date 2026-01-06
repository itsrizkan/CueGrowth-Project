import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
from main import app, TaskPayload

client = TestClient(app)

# ------------------------------
# Fixtures to mock NATS and Redis
# ------------------------------
@pytest.fixture(autouse=True)
def mock_nc_redis():
    with patch("main.nc", new_callable=AsyncMock) as mock_nc:
        with patch("main.redis_client", new_callable=MagicMock) as mock_redis:
            mock_nc.publish = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.dbsize.return_value = 5
            mock_redis.get.side_effect = lambda key: {
                "worker:processed_count": "10",
                "queue:backlog": "2",
                "api:tasks_published": "10"
            }.get(key, "0")
            yield

# ------------------------------
# Root endpoint tests
# ------------------------------
def test_root():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "api-gateway"

# ------------------------------
# Health endpoint tests
# ------------------------------
def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["nats"] is True
    assert data["valkey"] is True

# ------------------------------
# Task endpoint tests
# ------------------------------
def test_create_task():
    payload = {"task_id": "test123", "data": {"foo": "bar"}}
    response = client.post("/task", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["task_id"] == "test123"

# ------------------------------
# Stats endpoint tests
# ------------------------------
def test_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["valkey_keys_count"] == 5
    assert data["worker_processed_count"] == 10

# ------------------------------
# Metrics endpoint tests
# ------------------------------
def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert len(response.text) > 0
