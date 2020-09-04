'''
db operations from asyncpg
'''
from typing import Dict, Optional, Any
import sys
from asyncpg import Record, Connection
from asyncpg.transaction import Transaction
from asyncpg.pool import Pool
import logging

logger = logging.getLogger(__name__)

_pool: Optional[Pool] = None
def set_default_pool(pool: Pool) -> None:
    global _pool
    _pool = pool

def get_default_pool() -> Pool:
    assert _pool is not None, "no default pool set, call set_default_pool() first!"
    return _pool

class LocalTxStack:
    '''
    Coroutine local transaction stack, it maintains a dict of connection list
    '''
    def __init__(self):
        #self.transactions: List[Tuple[Pool, Connection]] = []
        self._transactions: Dict[str, List[Connection]] = {}

    def push_conn(self, conn: Connection, pool: Optional[Pool] = None):
        if pool is None:
            pool = get_default_pool()

        arr = self._transactions.get(pool, [])
        arr.append(conn)
        self._transactions[pool] = arr

    def pop_conn(self, pool: Optional[Pool] = None) -> Optional[Connection]:
        if pool is None:
            pool = get_default_pool()

        conn: Optional[Connection] = None
        arr = self._transactions.get(pool)
        if arr:
            arr, conn = arr[:-1], arr[-1]
            if arr:
                self._transactions[pool] = arr
            else:
                del self._transactions[pool]
        return conn

    def get_conn(self, pool: Optional[Pool] = None) -> Optional[Connection]:
        if pool is None:
            pool = get_default_pool()

        arr = self._transactions.get(pool)
        if arr:
            return arr[-1]
        return None

def contextvar_available() -> bool:
    return sys.version >= '3.7'

if contextvar_available():
    from contextvars import ContextVar
    txstack: ContextVar[LocalTxStack] = ContextVar(
        'txs', default=None)

    def get_localtx() -> LocalTxStack:
        localtx = txstack.get()
        if localtx is None:
            localtx = LocalTxStack()
            txstack.set(localtx)
        return localtx



class LocalTransaction:
    '''
    Coroutine local transaction, it depends on contextvars library which introduced in python 3.7
    Example usages:
    >>> async with local_transaction(pool, ...):
            await table('users').filter(...).update(...)
    '''
    _tx: Transaction
    _conn_proxy: Any
    _pool: Pool

    @staticmethod
    def get_conn(pool: Optional[Pool] = None) -> Optional[Connection]:
        '''
        try get connection from local contextvars, if the python version is too low, then return None

        :param pool: the db pool based with to look up a connection
        :returns: pushed connection if exists else None
        '''
        if contextvar_available():
            localtx = get_localtx()
            if localtx is None:
                localtx = LocalTxStack()
                txstack.set(localtx)
            return localtx.get_conn(pool)
        else:
            return None

    def __init__(self, pool: Optional[Pool] = None, **kwargs):
        '''
        :param kwargs: the arguments passed to conn.transaction()
        '''
        assert contextvar_available(), 'python version must be larger then 3.7 to support contextvars'
        if pool is None:
            pool = get_default_pool()
        self._pool = pool
        self.kwargs = kwargs

    async def __aenter__(self) -> Connection:
        localtx = get_localtx()
        self._conn_proxy = self._pool.acquire()
        conn = await self._conn_proxy.__aenter__()
        localtx.push_conn(conn, pool=self._pool)
        self._tx = conn.transaction(**self.kwargs)
        await self._tx.__aenter__()
        return conn

    async def __aexit__(self, exc_type, exc, tb):
        if self._tx:
            await self._tx.__aexit__(exc_type, exc, tb)

        if self._conn_proxy:
            await self._conn_proxy.__aexit__(exc_type, exc, tb)
            localtx = get_localtx()
            localtx.pop_conn()

        if exc_type is not None:
            logger.error('transaction error', exc_info=(exc_type, exc, tb))

local_transaction = LocalTransaction

