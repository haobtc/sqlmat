import asyncpg
import pytest

@pytest.fixture
async def dbpool():
    pool = await asyncpg.create_pool(
        user='dev',
        password='dev',
        database='sqlmattest',
        host='127.0.0.1')

    async with pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS "testuser"
        (
        id SERIAL,
        name VARCHAR(200),
        gender VARCHAR(32),
        info TEXT
        );
        """)
    return pool
