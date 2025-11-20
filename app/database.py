"""
Database connection utilities for PostgreSQL.
Provides connection pooling and async database operations.
"""
import os
from typing import Optional
import asyncpg
from asyncpg import Pool
from init_db import initialize_database

# Global connection pool
_pool: Optional[Pool] = None


async def init_db():
    """Initialize database connection pool on startup."""
    global _pool
    
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://airflow:airflow@postgres:5432/airflow"
    )
    
    # Initialize schema first
    await initialize_database()
    
    # Create connection pool with optimized settings
    _pool = await asyncpg.create_pool(
        database_url,
        min_size=2,          # Minimum connections in pool
        max_size=10,         # Maximum connections in pool
        max_queries=50000,   # Max queries per connection before recycling
        max_inactive_connection_lifetime=300,  # Close idle connections after 5 minutes
        command_timeout=60   # Query timeout in seconds
    )
    print("Database connection pool initialized")


async def close_db():
    """Close database connection pool on shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        print("Database connection pool closed")


def get_pool() -> Pool:
    """Get the database connection pool."""
    if _pool is None:
        raise RuntimeError("Database pool not initialized. Call init_db() first.")
    return _pool


async def execute_query(query: str, *args):
    """Execute a query and return results."""
    pool = get_pool()
    async with pool.acquire() as connection:
        return await connection.fetch(query, *args)


async def execute_command(query: str, *args):
    """Execute a command (INSERT, UPDATE, DELETE) without returning results."""
    pool = get_pool()
    async with pool.acquire() as connection:
        return await connection.execute(query, *args)
