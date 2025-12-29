from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import Response
import nats
import redis
import os
import json
import logging
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CueGrowth API Gateway")

# Prometheus metrics
request_counter = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method'])
request_duration = Histogram('api_request_duration_seconds', 'Request duration', ['endpoint'])
task_published = Counter('tasks_published_total', 'Total tasks published to queue')

# Global connections
nc = None
redis_client = None

class TaskPayload(BaseModel):
    """Model for task submission"""
    task_id: str
    data: dict

@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    global nc, redis_client
    
    # Get configuration from environment variables
    nats_url = os.getenv("NATS_URL", "nats://nats:4222")
    nats_user = os.getenv("NATS_USER", "")
    nats_password = os.getenv("NATS_PASSWORD", "")
    
    redis_host = os.getenv("REDIS_HOST", "valkey-master")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.getenv("REDIS_PASSWORD", "")
    
    try:
        # Connect to NATS
        if nats_user and nats_password:
            nc = await nats.connect(nats_url, user=nats_user, password=nats_password)
        else:
            nc = await nats.connect(nats_url)
        logger.info(f"Connected to NATS at {nats_url}")
        
        # Connect to Valkey/Redis
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            password=redis_password,
            decode_responses=True,
            socket_connect_timeout=5
        )
        # Test connection
        redis_client.ping()
        logger.info(f"Connected to Valkey at {redis_host}:{redis_port}")
        
    except Exception as e:
        logger.error(f"Failed to initialize connections: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup connections on shutdown"""
    global nc, redis_client
    
    if nc:
        await nc.close()
        logger.info("NATS connection closed")
    
    if redis_client:
        redis_client.close()
        logger.info("Redis connection closed")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "healthy", "service": "api-gateway"}

@app.get("/health")
async def health():
    """Readiness check - verifies all dependencies"""
    health_status = {
        "status": "healthy",
        "nats": False,
        "valkey": False
    }
    
    try:
        # Check NATS connection
        if nc and nc.is_connected:
            health_status["nats"] = True
        
        # Check Valkey connection
        if redis_client:
            redis_client.ping()
            health_status["valkey"] = True
        
        if health_status["nats"] and health_status["valkey"]:
            return health_status
        else:
            raise HTTPException(status_code=503, detail=health_status)
            
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail=str(e))

@app.post("/task")
async def create_task(task: TaskPayload):
    """
    Publish a task to the NATS queue
    
    Example payload:
    {
        "task_id": "task-123",
        "data": {
            "operation": "process",
            "value": 42
        }
    }
    """
    request_counter.labels(endpoint='/task', method='POST').inc()
    
    try:
        # Serialize task to JSON
        message = json.dumps(task.dict())
        
        # Publish to NATS subject "tasks"
        await nc.publish("tasks", message.encode())
        
        task_published.inc()
        logger.info(f"Task published: {task.task_id}")
        
        return {
            "status": "accepted",
            "task_id": task.task_id,
            "message": "Task queued for processing"
        }
        
    except Exception as e:
        logger.error(f"Failed to publish task: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")

@app.get("/stats")
async def get_stats():
    """
    Get system statistics
    
    Returns:
    - Valkey keys count
    - Queue backlog (approximated via Redis counter)
    - Worker processed count
    """
    request_counter.labels(endpoint='/stats', method='GET').inc()
    
    try:
        # Get Valkey statistics
        valkey_keys_count = redis_client.dbsize()
        
        # Get processed count from Valkey
        processed_count = redis_client.get("worker:processed_count")
        processed_count = int(processed_count) if processed_count else 0
        
        # Get queue backlog (stored by worker)
        queue_backlog = redis_client.get("queue:backlog")
        queue_backlog = int(queue_backlog) if queue_backlog else 0
        
        # Get total tasks published
        total_published = redis_client.get("api:tasks_published")
        total_published = int(total_published) if total_published else 0
        
        return {
            "valkey_keys_count": valkey_keys_count,
            "queue_backlog": queue_backlog,
            "worker_processed_count": processed_count,
            "total_tasks_published": total_published,
            "processing_rate": f"{(processed_count / max(total_published, 1) * 100):.2f}%"
        }
        
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to retrieve stats: {str(e)}")

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type="text/plain")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)