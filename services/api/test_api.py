import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
import json

from main import app, TaskPayload

client = TestClient(app)


# --- Root endpoint ---
def test_root():
    r = client.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "healthy"
    assert data["service"] == "api-gateway"


# --- Health endpoint ---
@patch("main.nc", new_callable=AsyncMock)
@patch("main.redis_client")
def test_health(mock_redis, mock_nats):
    mock_redis.ping.return_value = True
    mock_nats.is_connected = True
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert data["nats"] is True
    assert data["valkey"] is True


# --- Task endpoint ---
@patch("main.nc", new_callable=AsyncMock)
def test_create_task(mock_nats):
    payload = {"task_id": "test1", "data": {"key": "value"}}
    r = client.post("/task", json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"
    assert data["task_id"] == "test1"


# --- Stats endpoint ---
@patch("main.redis_client")
def test_stats(mock_redis):
    mock_redis.dbsize.return_value = 10
    mock_redis.get.side_effect = lambda k: {"worker:processed_count":"5","queue:backlog":"2","api:tasks_published":"5"}.get(k,"0")
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    assert data["valkey_keys_count"] == 10
    assert data["worker_processed_count"] == 5


# --- Metrics endpoint ---
def test_metrics():
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"]


# --- TaskPayload model tests ---
def test_taskpayload_model():
    task = TaskPayload(task_id="abc", data={"x":1})
    assert task.task_id == "abc"
    assert task.data["x"] == 1
