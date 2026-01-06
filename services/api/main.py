# main.py
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
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
request_counter = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method'])
request_duration = Histogram('api_request_duration_seconds', 'Request duration', ['endpoint'])
task_published = Counter('tasks_published_total', 'Total tasks published to queue')


# Task model
class TaskPayload(BaseModel):
    task_id: str
    data: dict


def create_app() -> FastAPI:
    """Factory function to create FastAPI app"""
    app = FastAPI(title="CueGrowth API Gateway")

    # Global connections
    nc: Optional[nats.aio.client.Client] = None
    redis_client: Optional[redis.Redis] = None

    # ------------------------------
    # Lifespan for startup/shutdown
    # ------------------------------
    @app.on_event("startup")
    async def startup_event():
        nonlocal nc, redis_client
        nats_url = os.getenv("NATS_URL", "nats://nats.cuegrowth.svc.cluster.local:4222")
        nats_user = os.getenv("NATS_USER", "")
        nats_password = os.getenv("NATS_PASSWORD", "")

        redis_host = os.getenv("REDIS_HOST", "valkey-master")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_password = os.getenv("REDIS_PASSWORD", "")

        # NATS connect
        for attempt in range(5):
            try:
                if nats_user and nats_password:
                    nc = await nats.connect(
                        servers=[nats_url], user=nats_user, password=nats_password, connect_timeout=5
                    )
                else:
                    nc = await nats.connect(servers=[nats_url], connect_timeout=5)
                logger.info(f"✅ Connected to NATS at {nats_url}")
                break
            except Exception as e:
                logger.warning(f"NATS connection attempt {attempt + 1}/5 failed: {e}")
                await asyncio.sleep(2)
        else:
            logger.error("❌ Failed to connect to NATS")
            raise RuntimeError("Failed to connect to NATS")

        # Redis connect
        try:
            redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                password=redis_password if redis_password else None,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            redis_client.ping()
            logger.info(f"✅ Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Redis: {e}")
            raise RuntimeError("Failed to connect to Redis")

    @app.on_event("shutdown")
    async def shutdown_event():
        nonlocal nc, redis_client
        if nc:
            await nc.close()
            logger.info("NATS connection closed")
        if redis_client:
            redis_client.close()
            logger.info("Redis connection closed")

    # ------------------------------
    # Endpoints
    # ------------------------------
    @app.get("/")
    async def root():
        return {"status": "healthy", "service": "api-gateway"}

    @app.get("/health")
    async def health():
        health_status = {"status": "healthy", "nats": False, "valkey": False}
        try:
            if nc and nc.is_connected:
                health_status["nats"] = True
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
        request_counter.labels(endpoint='/task', method='POST').inc()
        try:
            message = json.dumps(task.model_dump())
            await nc.publish("tasks", message.encode())
            task_published.inc()
            logger.info(f"Task published: {task.task_id}")
            return {"status": "accepted", "task_id": task.task_id, "message": "Task queued for processing"}
        except Exception as e:
            logger.error(f"Failed to publish task: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to queue task: {str(e)}")

    @app.get("/stats")
    async def get_stats():
        request_counter.labels(endpoint='/stats', method='GET').inc()
        try:
            valkey_keys_count = redis_client.dbsize() if redis_client else 0
            processed_count = int(redis_client.get("worker:processed_count") or 0) if redis_client else 0
            queue_backlog = int(redis_client.get("queue:backlog") or 0) if redis_client else 0
            total_published = int(redis_client.get("api:tasks_published") or 0) if redis_client else 0
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
        return Response(content=generate_latest(), media_type="text/plain")

    return app


# Create app instance for uvicorn
app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
