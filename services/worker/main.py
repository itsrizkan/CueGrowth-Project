import asyncio
import nats
import redis
import json
import os
import signal
import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Prometheus metrics
tasks_processed = Counter('worker_tasks_processed_total', 'Total tasks processed')
tasks_failed = Counter('worker_tasks_failed_total', 'Total tasks failed')
processing_duration = Histogram('worker_task_duration_seconds', 'Task processing duration')
queue_backlog = Gauge('worker_queue_backlog', 'Current queue backlog')

class GracefulShutdown:
    """Handle graceful shutdown"""
    def __init__(self):
        self.shutdown_flag = False
        
    def shutdown(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_flag = True

class Worker:
    """Worker that processes tasks from NATS queue"""
    
    def __init__(self):
        self.nc = None
        self.redis_client = None
        self.subscription = None
        self.shutdown_handler = GracefulShutdown()
        self.processing_count = 0
        
        # Register signal handlers
        signal.signal(signal.SIGTERM, self.shutdown_handler.shutdown)
        signal.signal(signal.SIGINT, self.shutdown_handler.shutdown)
    
    async def connect(self):
        """Establish connections to NATS and Valkey"""
        # Get configuration from environment
        nats_url = os.getenv("NATS_URL", "nats://nats:4222")
        nats_user = os.getenv("NATS_USER", "")
        nats_password = os.getenv("NATS_PASSWORD", "")
        
        redis_host = os.getenv("REDIS_HOST", "valkey-master")
        redis_port = int(os.getenv("REDIS_PORT", "6379"))
        redis_password = os.getenv("REDIS_PASSWORD", "")
        
        retry_count = 0
        max_retries = 10
        
        while retry_count < max_retries:
            try:
                # Connect to NATS
                if nats_user and nats_password:
                    self.nc = await nats.connect(
                        nats_url,
                        user=nats_user,
                        password=nats_password,
                        max_reconnect_attempts=-1  # Infinite reconnect
                    )
                else:
                    self.nc = await nats.connect(
                        nats_url,
                        max_reconnect_attempts=-1
                    )
                logger.info(f"Connected to NATS at {nats_url}")
                
                # Connect to Valkey/Redis
                self.redis_client = redis.Redis(
                    host=redis_host,
                    port=redis_port,
                    password=redis_password,
                    decode_responses=True,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    health_check_interval=30
                )
                # Test connection
                self.redis_client.ping()
                logger.info(f"Connected to Valkey at {redis_host}:{redis_port}")
                
                return
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Connection failed (attempt {retry_count}/{max_retries}): {e}")
                if retry_count < max_retries:
                    await asyncio.sleep(5)
                else:
                    raise
    
    async def process_message(self, msg):
        """Process a single message from the queue"""
        start_time = time.time()
        
        try:
            # Decode message
            data = json.loads(msg.data.decode())
            task_id = data.get('task_id', 'unknown')
            
            logger.info(f"Processing task: {task_id}")
            
            # Simulate processing (replace with actual logic)
            await asyncio.sleep(0.1)  # Simulate work
            
            # Store result in Valkey
            result_key = f"result:{task_id}"
            result_data = {
                "task_id": task_id,
                "status": "completed",
                "processed_at": time.time(),
                "data": data.get('data', {})
            }
            
            self.redis_client.setex(
                result_key,
                3600,  # TTL: 1 hour
                json.dumps(result_data)
            )
            
            # Increment processed counter
            self.redis_client.incr("worker:processed_count")
            
            # Update metrics
            tasks_processed.inc()
            processing_duration.observe(time.time() - start_time)
            
            logger.info(f"Task completed: {task_id} in {time.time() - start_time:.3f}s")
            
            # Acknowledge message (at-least-once delivery)
            await msg.ack()
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}")
            tasks_failed.inc()
            await msg.nak()  # Negative acknowledgment - requeue
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            tasks_failed.inc()
            await msg.nak()  # Requeue for retry
    
    async def subscribe(self):
        """Subscribe to NATS subject and process messages"""
        try:
            # Create JetStream context for persistence
            js = self.nc.jetstream()
            
            # Subscribe to "tasks" subject with queue group
            self.subscription = await js.subscribe(
                "tasks",
                queue="workers",
                durable="worker-durable",
                manual_ack=True  # Manual acknowledgment for at-least-once
            )
            
            logger.info("Subscribed to 'tasks' subject")
            
            # Process messages
            while not self.shutdown_handler.shutdown_flag:
                try:
                    # Get message with timeout
                    msg = await asyncio.wait_for(
                        self.subscription.next_msg(),
                        timeout=1.0
                    )
                    
                    # Process message
                    await self.process_message(msg)
                    
                    # Update queue backlog metric
                    pending = await self.subscription.pending_msgs()
                    queue_backlog.set(pending)
                    
                    # Store backlog in Redis for API stats
                    self.redis_client.set("queue:backlog", pending)
                    
                except asyncio.TimeoutError:
                    # No message available, continue
                    continue
                    
                except Exception as e:
                    logger.error(f"Error in message loop: {e}")
                    await asyncio.sleep(1)
            
            logger.info("Shutdown flag detected, stopping message processing")
            
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            raise
    
    async def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down worker...")
        
        # Unsubscribe
        if self.subscription:
            await self.subscription.unsubscribe()
            logger.info("Unsubscribed from NATS")
        
        # Close connections
        if self.nc:
            await self.nc.drain()  # Wait for pending messages
            await self.nc.close()
            logger.info("NATS connection closed")
        
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")
    
    async def run(self):
        """Main worker loop"""
        try:
            # Start Prometheus metrics server
            start_http_server(8001)
            logger.info("Metrics server started on port 8001")
            
            # Connect to services
            await self.connect()
            
            # Subscribe and process messages
            await self.subscribe()
            
        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise
        finally:
            await self.shutdown()

async def main():
    """Entry point"""
    worker = Worker()
    await worker.run()

if __name__ == "__main__":
    asyncio.run(main())