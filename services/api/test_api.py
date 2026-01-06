import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
import asyncio
from main import app, TaskPayload, lifespan

client = TestClient(app)

# ------------------------------
# Fixture: mock NATS & Redis for all tests
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
    data = r.json()
    assert data["status"] == "healthy"
    assert data["service"] == "api-gateway"

# ------------------------------
# Health endpoint
# ------------------------------
def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["nats"] is True
    assert data["valkey"] is True

def test_health_nc_none():
    with patch("main.nc", None):
        r = client.get("/health")
        # NATS missing → 503
        assert r.status_code == 503

def test_health_redis_none():
    with patch("main.redis_client", None):
        r = client.get("/health")
        # Redis missing → 503
        assert r.status_code == 503

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
    with patch("main.nc") as mock_nc:
        mock_nc.publish = AsyncMock(side_effect=Exception("boom"))
        r = client.post("/task", json=payload)
        assert r.status_code == 500
        assert "Failed to queue task" in r.json()["detail"]

def test_task_nc_none():
    payload = {"task_id": "none123", "data": {"foo": "bar"}}
    with patch("main.nc", None):
        r = client.post("/task", json=payload)
        # Even without nc, task_published.inc() runs
        assert r.status_code == 200

def test_task_invalid_payload():
    r = client.post("/task", json={})
    assert r.status_code == 422

# ------------------------------
# Stats endpoint
# ------------------------------
def test_stats_normal():
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    assert data["worker_processed_count"] == 10
    assert data["queue_backlog"] == 2
    assert data["valkey_keys_count"] == 5

def test_stats_redis_none():
    with patch("main.redis_client", None):
        r = client.get("/stats")
        data = r.json()
        assert data["worker_processed_count"] == 0
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

# ------------------------------
# Pydantic TaskPayload model
# ------------------------------
def test_taskpayload_model_valid():
    task = TaskPayload(task_id="x", data={"foo": "bar"})
    assert task.task_id == "x"
    assert task.data == {"foo": "bar"}

def test_taskpayload_model_missing_task_id():
    with pytest.raises(Exception):
        TaskPayload(data={"foo": "bar"})

def test_taskpayload_model_missing_data():
    with pytest.raises(Exception):
        TaskPayload(task_id="x")

# ------------------------------
# FastAPI lifespan coverage (startup/shutdown)
# ------------------------------
def test_lifespan_execution():
    async def run_lifespan():
        async with lifespan(app):
            # This triggers startup/shutdown code
            pass
    asyncio.run(run_lifespan())
