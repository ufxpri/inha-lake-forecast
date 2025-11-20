"""
Database initialization script.
Creates tables and indexes on application startup.
"""
import os
import asyncpg
from pathlib import Path


async def initialize_database():
    """
    Initialize database schema by executing schema.sql.
    This function is called during application startup.
    """
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql://airflow:airflow@postgres:5432/airflow"
    )
    
    # Read schema file
    schema_path = Path(__file__).parent / "schema.sql"
    with open(schema_path, "r") as f:
        schema_sql = f.read()
    
    # Connect and execute schema
    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(schema_sql)
        print("Database schema initialized successfully")
    except Exception as e:
        print(f"Error initializing database schema: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(initialize_database())
