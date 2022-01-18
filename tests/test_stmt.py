import pytest
import asyncpg
from sqlmat import table, F, set_default_pool, get_default_pool, local_transaction, DBRow
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

class TUser(DBRow):
    id: int
    name: str
    gender: str
    info: str

@pytest.mark.asyncio
async def test_get_iter(dbpool):
    set_default_pool(dbpool)

    tbl = table('testuser')
    await tbl.using(None).insert(
        name='kitty', gender='female')
    await tbl.using(None).insert(
        name='hello', gender='male')
    await tbl.using(None).insert(
        name='cosmos', gender='female')
    try:
        async with local_transaction():
            rqs = tbl.filter().order_by('id').get_iter()
            async for r in rqs:
                if r['name'] == 'kitty':
                    assert r['gender'] == 'female'
                elif r['name'] == 'cosmos':
                    assert r['gender'] == 'female'

            qs = tbl.filter().order_by('id').get_iter_as(TUser)
            async for r in qs:
                if r.name == 'kitty':
                    assert r['gender'] == 'female'
                elif r.name == 'cosmos':
                    assert r.gender == 'female'
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

