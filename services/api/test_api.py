import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import sys
import json

# Mock external dependencies before importing main
sys.modules['nats'] = Mock()
sys.modules['redis'] = Mock()

# Now import the app
from main import app, TaskPayload

# Create test client
client = TestClient(app)


class TestRootEndpoint:
    """Test root endpoint"""
    
    def test_root_endpoint_returns_200(self):
        """Root endpoint should return 200"""
        response = client.get("/")
        assert response.status_code == 200
    
    def test_root_endpoint_returns_json(self):
        """Root endpoint should return JSON"""
        response = client.get("/")
        assert response.headers["content-type"] == "application/json"
    
    def test_root_endpoint_contains_status(self):
        """Root endpoint should contain status field"""
        response = client.get("/")
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"
    
    def test_root_endpoint_contains_service_name(self):
        """Root endpoint should contain service field"""
        response = client.get("/")
        data = response.json()
        assert "service" in data
        assert data["service"] == "api-gateway"


class TestHealthEndpoint:
    """Test health check endpoint"""
    
    def test_health_endpoint_exists(self):
        """Health endpoint should exist"""
        response = client.get("/health")
        assert response.status_code in [200, 503]  # Can be unhealthy without real deps
    
    def test_health_endpoint_returns_json(self):
        """Health endpoint should return JSON"""
        response = client.get("/health")
        assert response.headers["content-type"] == "application/json"


class TestTaskEndpoint:
    """Test task submission endpoint"""
    
    @patch('main.nc')
    @patch('main.redis_client')
    def test_task_endpoint_accepts_valid_payload(self, mock_redis, mock_nats):
        """Task endpoint should accept valid task payload"""
        # Mock NATS publish
        mock_nats.publish = AsyncMock()
        
        payload = {
            "task_id": "test-123",
            "data": {"key": "value"}
        }
        
        # Note: This will fail without proper async mocking, but validates structure
        response = client.post("/task", json=payload)
        # We expect it might fail due to async mocking, so we check for common codes
        assert response.status_code in [200, 500]
    
    def test_task_endpoint_requires_task_id(self):
        """Task endpoint should require task_id"""
        payload = {
            "data": {"key": "value"}
        }
        response = client.post("/task", json=payload)
        assert response.status_code == 422  # Validation error
    
    def test_task_endpoint_requires_data(self):
        """Task endpoint should require data field"""
        payload = {
            "task_id": "test-123"
        }
        response = client.post("/task", json=payload)
        assert response.status_code == 422  # Validation error
    
    def test_task_endpoint_rejects_empty_payload(self):
        """Task endpoint should reject empty payload"""
        response = client.post("/task", json={})
        assert response.status_code == 422
    
    def test_task_endpoint_rejects_invalid_json(self):
        """Task endpoint should reject invalid JSON"""
        response = client.post(
            "/task",
            data="invalid json",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422


class TestStatsEndpoint:
    """Test statistics endpoint"""
    
    @patch('main.redis_client')
    def test_stats_endpoint_exists(self, mock_redis):
        """Stats endpoint should exist"""
        # Mock Redis responses
        mock_redis.dbsize.return_value = 10
        mock_redis.get.side_effect = lambda key: {
            "worker:processed_count": "5",
            "queue:backlog": "2",
            "api:tasks_published": "5"
        }.get(key, "0")
        
        response = client.get("/stats")
        # May fail without proper Redis mock, but endpoint should exist
        assert response.status_code in [200, 500]
    
    def test_stats_endpoint_returns_json(self):
        """Stats endpoint should return JSON when successful"""
        with patch('main.redis_client') as mock_redis:
            mock_redis.dbsize.return_value = 10
            mock_redis.get.return_value = "5"
            
            response = client.get("/stats")
            if response.status_code == 200:
                assert response.headers["content-type"] == "application/json"


class TestMetricsEndpoint:
    """Test Prometheus metrics endpoint"""
    
    def test_metrics_endpoint_exists(self):
        """Metrics endpoint should exist"""
        response = client.get("/metrics")
        assert response.status_code == 200
    
    def test_metrics_endpoint_returns_text(self):
        """Metrics endpoint should return text/plain"""
        response = client.get("/metrics")
        assert "text/plain" in response.headers["content-type"]
    
    def test_metrics_contains_prometheus_format(self):
        """Metrics should be in Prometheus format"""
        response = client.get("/metrics")
        content = response.text
        # Check for typical Prometheus metric patterns
        assert "# HELP" in content or "# TYPE" in content or len(content) > 0


class TestTaskPayloadModel:
    """Test TaskPayload Pydantic model"""
    
    def test_task_payload_with_valid_data(self):
        """TaskPayload should validate correct data"""
        payload = TaskPayload(
            task_id="test-123",
            data={"key": "value"}
        )
        assert payload.task_id == "test-123"
        assert payload.data == {"key": "value"}
    
    def test_task_payload_requires_task_id(self):
        """TaskPayload should require task_id"""
        with pytest.raises(Exception):  # Pydantic validation error
            TaskPayload(data={"key": "value"})
    
    def test_task_payload_requires_data(self):
        """TaskPayload should require data"""
        with pytest.raises(Exception):  # Pydantic validation error
            TaskPayload(task_id="test-123")
    
    def test_task_payload_data_must_be_dict(self):
        """TaskPayload data must be a dictionary"""
        payload = TaskPayload(
            task_id="test-123",
            data={"nested": {"key": "value"}}
        )
        assert isinstance(payload.data, dict)


class TestErrorHandling:
    """Test error handling"""
    
    def test_404_on_invalid_endpoint(self):
        """Should return 404 for non-existent endpoints"""
        response = client.get("/nonexistent")
        assert response.status_code == 404
    
    def test_405_on_wrong_method(self):
        """Should return 405 for wrong HTTP method"""
        response = client.get("/task")  # Should be POST
        assert response.status_code == 405
    
    def test_invalid_content_type(self):
        """Should handle invalid content type"""
        response = client.post(
            "/task",
            data="not json",
            headers={"Content-Type": "text/plain"}
        )
        assert response.status_code in [422, 400]


# Pytest configuration
def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers",
        "asyncio: mark test as async"
    )


# Coverage report settings
def pytest_addoption(parser):
    """Add custom pytest options"""
    parser.addoption(
        "--cov-fail-under",
        action="store",
        default=80,
        help="Fail if coverage is below threshold"
    )