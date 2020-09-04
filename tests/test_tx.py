import pytest
import asyncpg
from sqlmat import table, F
from sqlmat import table, F, set_default_pool, get_default_pool, local_transaction

from .fixtures import dbpool

class AbortTransaction(Exception):
    pass

@pytest.mark.asyncio
async def test_rollback(dbpool):
    set_default_pool(dbpool)

    tbl = table('testuser')
    try:
        await tbl.insert(
            name='mike', gender='male')
        try:
            async with local_transaction() as _conn:
                r = await tbl.filter(
                    name='mike').update(gender='female')

                assert r['gender'] == 'female'
                # abort the transation, rollback the gender changing
                raise AbortTransaction()
        except AbortTransaction:
            pass

        r = await tbl.filter(name='mike').get_one()
        assert r['gender'] == 'male'
    finally:
        await tbl.delete()

@pytest.mark.asyncio
async def test_nest_tx(dbpool):
    set_default_pool(dbpool)

    tbl = table('testuser')

    try:
        await tbl.insert(
            name='mike', gender='male', info='info 01')

        await tbl.insert(
            name='marry', gender='female', info='info 11')

        try:
            async with local_transaction(isolation='repeatable_read') as _c1:
                r = await tbl.filter(
                    name='mike').update(info='info 02')
                async with local_transaction(isolation='repeatable_read') as _c2:
                    r = await tbl.filter(
                        name='marry').update(info='info 12')

                    assert r['info'] == 'info 12'

                rm = await tbl.filter(name='marry').get_one()
                # isolation is repeatable_read
                assert rm['info'] == 'info 11'
                raise AbortTransaction()
        except AbortTransaction:
            pass

        r = await tbl.filter(name='marry').get_one()
        assert r['info'] == 'info 12'
        r = await tbl.filter(name='mike').get_one()
        assert r['info'] == 'info 01'
    finally:
        await tbl.delete()
