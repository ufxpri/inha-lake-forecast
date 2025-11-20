"""
Redis connection utilities for caching.
Provides async Redis operations for score caching and data retrieval.
"""
import os
import json
from typing import Optional, Any
import redis.asyncio as redis

# Global Redis client
_redis_client: Optional[redis.Redis] = None


async def init_redis():
    """Initialize Redis connection on startup."""
    global _redis_client
    
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    
    _redis_client = redis.Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=True
    )
    
    # Test connection
    await _redis_client.ping()
    print("Redis connection initialized")


async def close_redis():
    """Close Redis connection on shutdown."""
    global _redis_client
    if _redis_client:
        await _redis_client.close()
        print("Redis connection closed")


def get_redis() -> redis.Redis:
    """Get the Redis client."""
    if _redis_client is None:
        raise RuntimeError("Redis client not initialized. Call init_redis() first.")
    return _redis_client


async def get_cached_data(key: str) -> Optional[dict]:
    """Get cached data from Redis and parse as JSON."""
    client = get_redis()
    data = await client.get(key)
    if data:
        return json.loads(data)
    return None


async def set_cached_data(key: str, value: Any, ttl: int = 3600):
    """Set cached data in Redis with TTL (default 1 hour)."""
    client = get_redis()
    await client.setex(key, ttl, json.dumps(value))


async def get_cached_html(key: str) -> Optional[str]:
    """Get cached HTML from Redis."""
    client = get_redis()
    return await client.get(key)


async def set_cached_html(key: str, html: str, ttl: int = 3600):
    """Set cached HTML in Redis with TTL."""
    client = get_redis()
    await client.setex(key, ttl, html)
