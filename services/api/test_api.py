import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
from main import app

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
# Root endpoint
# ------------------------------
def test_root():
    r = client.get("/")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"

# ------------------------------
# Health endpoint
# ------------------------------
def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["nats"] is True
    assert data["valkey"] is True

# ------------------------------
# Task endpoint
# ------------------------------
def test_create_task():
    payload = {"task_id": "test123", "data": {"foo": "bar"}}
    r = client.post("/task", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"

def test_task_publish_failure():
    payload = {"task_id": "fail123", "data": {"foo": "bar"}}
    with patch("main.nc", new_callable=AsyncMock) as mock_nc:
        mock_nc.publish.side_effect = Exception("NATS down")
        r = client.post("/task", json=payload)
        assert r.status_code == 500
        assert "Failed to queue task" in r.json()["detail"]

# ------------------------------
# Stats endpoint
# ------------------------------
def test_stats():
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    assert data["worker_processed_count"] == 10

def test_stats_with_empty_db():
    with patch("main.redis_client", new_callable=MagicMock) as mock_redis:
        mock_redis.dbsize.return_value = 0
        mock_redis.get.return_value = None
        r = client.get("/stats")
        assert r.status_code == 200
        data = r.json()
        assert data["valkey_keys_count"] == 0

def test_stats_redis_failure():
    with patch("main.redis_client", new_callable=MagicMock) as mock_redis:
        mock_redis.dbsize.side_effect = Exception("Redis down")
        r = client.get("/stats")
        assert r.status_code == 500
        assert "Failed to retrieve stats" in r.json()["detail"]

# ------------------------------
# Metrics endpoint
# ------------------------------
def test_metrics():
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]
