import pytest
import asyncpg
from sqlmat import table, F, set_default_pool, get_default_pool, local_transaction
from .fixtures import dbpool

@pytest.mark.asyncio
async def test_select(dbpool):
    set_default_pool(dbpool)

    tbl = table('testuser')
    await tbl.using(None).insert(
        name='mike', gender='male')

    try:
        r = await tbl.filter(
            name='mike').get_one()
        assert r['gender'] == 'male'
    finally:
        await tbl.delete()

@pytest.mark.asyncio
async def test_update(dbpool):
    set_default_pool(dbpool)

    tbl = table('testuser')
    await tbl.insert(
        name='jake', gender='male', info='a good man')
    try:
        r = await tbl.filter(name='jake').update(
            info='a bad man')

        assert r['info'] == 'a bad man'

    finally:
        await tbl.delete()

