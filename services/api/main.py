from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
import nats
from nats.errors import TimeoutError as NatsTimeoutError
import redis
import os
import json
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app
import logging
import asyncio

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="CueGrowth API Gateway",
    description="Task submission and statistics API",
    version="1.0.0"
)

# Prometheus metrics
tasks_created = Counter('api_tasks_created_total', 'Total tasks created')
api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method', 'status'])
api_latency = Histogram('api_request_duration_seconds', 'API request latency', ['endpoint'])
queue_backlog = Gauge('api_queue_backlog', 'Current queue backlog')
valkey_keys = Gauge('api_valkey_keys_total', 'Total keys in Valkey')

# Configuration from environment
NATS_URL = os.getenv('NATS_URL', 'nats://nats:4222')
NATS_USER = os.getenv('NATS_USER', 'cuegrowth')
NATS_PASS = os.getenv('NATS_PASS', 'NatsSecurePass123!')
VALKEY_HOST = os.getenv('VALKEY_HOST', 'valkey')
VALKEY_PORT = int(os.getenv('VALKEY_PORT', '6379'))
VALKEY_PASS = os.getenv('VALKEY_PASS', 'ValkeySecurePass123!')

# Global connections
nc = None
js = None
redis_client = None

class TaskPayload(BaseModel):
    task_type: str
    data: dict
    priority: int = 0

    class Config:
        json_schema_extra = {
            "example": {
                "task_type": "data_processing",
                "data": {"key": "value", "items": [1, 2, 3]},
                "priority": 0
            }
        }

@app.on_event("startup")
async def startup():
    global nc, js, redis_client
    logger.info("Starting API Gateway...")
    
    try:
        # Connect to NATS
        logger.info(f"Connecting to NATS at {NATS_URL}")
        nc = await nats.connect(
            servers=[NATS_URL],
            user=NATS_USER,
            password=NATS_PASS,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2
        )
        js = nc.jetstream()
        
        # Ensure JetStream stream exists
        try:
            await js.stream_info("TASKS")
            logger.info("JetStream stream 'TASKS' already exists")
        except:
            logger.info("Creating JetStream stream 'TASKS'")
            await js.add_stream(
                name="TASKS",
                subjects=["tasks.>"],
                retention="limits",
                max_msgs=100000,
                max_bytes=1024*1024*1024,  # 1GB
                storage="file"
            )
        
        logger.info("Connected to NATS successfully")
        
        # Connect to Redis/Valkey
        logger.info(f"Connecting to Valkey at {VALKEY_HOST}:{VALKEY_PORT}")
        redis_client = redis.Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PASS,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True
        )
        
        # Test connection
        redis_client.ping()
        logger.info("Connected to Valkey successfully")
        
    except Exception as e:
        logger.error(f"Startup error: {e}")
        raise

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down API Gateway...")
    if nc:
        await nc.close()
    if redis_client:
        redis_client.close()
    logger.info("Shutdown complete")

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "api-gateway",
        "version": "1.0.0"
    }

@app.get("/ready")
async def ready():
    """Readiness check endpoint"""
    try:
        # Check NATS connection
        if not nc or not nc.is_connected:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="NATS not connected"
            )
        
        # Check Valkey connection
        redis_client.ping()
        
        api_requests.labels(endpoint='/ready', method='GET', status='success').inc()
        return {
            "status": "ready",
            "nats": "connected",
            "valkey": "connected"
        }
    except redis.ConnectionError:
        api_requests.labels(endpoint='/ready', method='GET', status='error').inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Valkey not connected"
        )
    except Exception as e:
        api_requests.labels(endpoint='/ready', method='GET', status='error').inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service not ready: {str(e)}"
        )

@app.post("/task", status_code=status.HTTP_201_CREATED)
async def create_task(payload: TaskPayload):
    """Submit a new task to the queue"""
    with api_latency.labels(endpoint='/task').time():
        try:
            message = json.dumps(payload.dict())
            
            # Publish to NATS JetStream
            ack = await js.publish(
                "tasks.new",
                message.encode(),
                timeout=5.0
            )
            
            tasks_created.inc()
            api_requests.labels(endpoint='/task', method='POST', status='success').inc()
            
            logger.info(f"Task created: seq={ack.seq}, type={payload.task_type}")
            
            return {
                "status": "queued",
                "task_id": f"task-{ack.seq}",
                "sequence": ack.seq,
                "stream": ack.stream
            }
            
        except NatsTimeoutError:
            api_requests.labels(endpoint='/task', method='POST', status='error').inc()
            logger.error("NATS publish timeout")
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Queue publish timeout"
            )
        except Exception as e:
            api_requests.labels(endpoint='/task', method='POST', status='error').inc()
            logger.error(f"Task creation error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to create task: {str(e)}"
            )

@app.get("/stats")
async def get_stats():
    """Get system statistics"""
    with api_latency.labels(endpoint='/stats').time():
        try:
            # Get Valkey stats
            keys_count = redis_client.dbsize()
            valkey_info = redis_client.info('stats')
            
            # Get queue stats
            try:
                stream_info = await js.stream_info("TASKS")
                backlog = stream_info.state.messages
                queue_backlog.set(backlog)
            except Exception as e:
                logger.warning(f"Could not get stream info: {e}")
                backlog = 0
            
            # Get worker processed count
            try:
                processed_count = redis_client.get("worker:processed_count")
                processed = int(processed_count) if processed_count else 0
            except:
                processed = 0
            
            # Update Prometheus metrics
            valkey_keys.set(keys_count)
            
            api_requests.labels(endpoint='/stats', method='GET', status='success').inc()
            
            return {
                "valkey": {
                    "keys_count": keys_count,
                    "total_commands": valkey_info.get('total_commands_processed', 0),
                    "connected_clients": valkey_info.get('connected_clients', 0)
                },
                "queue": {
                    "backlog_messages": backlog,
                    "stream": "TASKS"
                },
                "workers": {
                    "processed_count": processed
                }
            }
            
        except redis.ConnectionError as e:
            api_requests.labels(endpoint='/stats', method='GET', status='error').inc()
            logger.error(f"Valkey connection error: {e}")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Valkey unavailable"
            )
        except Exception as e:
            api_requests.labels(endpoint='/stats', method='GET', status='error').inc()
            logger.error(f"Stats error: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to get stats: {str(e)}"
            )

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "service": "CueGrowth API Gateway",
        "version": "1.0.0",
        "endpoints": {
            "POST /task": "Submit a new task",
            "GET /stats": "Get system statistics",
            "GET /health": "Health check",
            "GET /ready": "Readiness check",
            "GET /metrics": "Prometheus metrics"
        }
    }