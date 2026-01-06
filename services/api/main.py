# services/api/test_api.py
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
import json

# Import app and TaskPayload
from main import app, TaskPayload

# ------------------------------
# Test client
# ------------------------------
client = TestClient(app)

# ------------------------------
# Fixtures to mock external dependencies
# ------------------------------
@pytest.fixture(autouse=True)
def mock_nc_redis():
    """Mock NATS and Redis globally for all tests"""
    with patch("main.nc", new_callable=AsyncMock) as mock_nc:
        with patch("main.redis_client", new_callable=MagicMock) as mock_redis:
            # NATS publish mock
            mock_nc.publish = AsyncMock()
            # Redis mock
            mock_redis.ping.return_value = True
            mock_redis.dbsize.return_value = 10
            mock_redis.get.side_effect = lambda key: {
                "worker:processed_count": "5",
                "queue:backlog": "2",
                "api:tasks_published": "5"
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
    assert data["status"] == "healthy"
    assert data["nats"] is True
    assert data["valkey"] is True

# ------------------------------
# Task endpoint tests
# ------------------------------
def test_create_task():
    payload = {"task_id": "task-1", "data": {"key": "value"}}
    response = client.post("/task", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "accepted"
    assert data["task_id"] == "task-1"
    assert "message" in data

def test_task_invalid_payload():
    # Missing task_id
    payload = {"data": {"key": "value"}}
    response = client.post("/task", json=payload)
    assert response.status_code == 422

    # Missing data
    payload = {"task_id": "task-2"}
    response = client.post("/task", json=payload)
    assert response.status_code == 422

def test_task_publish_failure():
    # Simulate NATS publish failure
    from main import nc
    nc.publish.side_effect = Exception("NATS down")

    payload = {"task_id": "task-3", "data": {"key": "value"}}
    response = client.post("/task", json=payload)
    assert response.status_code == 500
    assert "Failed to queue task" in response.json()["detail"]

# ------------------------------
# Stats endpoint tests
# ------------------------------
def test_stats():
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert data["valkey_keys_count"] == 10
    assert data["queue_backlog"] == 2
    assert data["worker_processed_count"] == 5
    assert data["total_tasks_published"] == 5
    assert data["processing_rate"] == "100.00%"

def test_stats_redis_failure():
    from main import redis_client
    redis_client.dbsize.side_effect = Exception("Redis down")
    response = client.get("/stats")
    assert response.status_code == 500

# ------------------------------
# Metrics endpoint tests
# ------------------------------
def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    # Check that Prometheus metric names are present
    assert "api_requests_total" in response.text
    assert "tasks_published_total" in response.text
