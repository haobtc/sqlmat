'''
db operations from asyncpg
'''
from typing import Dict, Optional, Any
import logging
import sys
import asyncio
from collections import defaultdict
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


class TxFrame:
    '''
    Coroutine local transaction frame
    '''
    def __init__(self):
        self.conn_proxy = None
        self.conn = None
        self.transactions: List[Transaction] = []

    def __str__(self):
        return '<Frame {} {} {}>'.format(self.conn_proxy,
                                      self.conn,
                                      self.transactions)

class FrameMap:
    '''
    A map of TxFrames for each pool
    '''
    #_task_name: str  # record the current name
    entries: Dict[Pool, TxFrame]

    def __init__(self):
        #self._task_name = current_task_name()
        self._ctask = asyncio.current_task()
        self.entries = defaultdict(TxFrame)

    def in_current_task(self) -> bool:
        #return self._task_name == current_task_name()
        return self._ctask == asyncio.current_task()

    def get_frame(self, pool: Pool) -> 'TxFrame':
        return self.entries[pool]

def contextvar_available() -> bool:
    return sys.version >= '3.7'

if contextvar_available():
    from contextvars import ContextVar
    cv: ContextVar[Optional[FrameMap]] = ContextVar('sqlmat_framemap', default=None)

    def _get_framemap() -> 'FrameMap':
        frame_map = cv.get()
        # copy_context may move the frame map to other task
        if frame_map is None or not frame_map.in_current_task():
            frame_map = FrameMap()
            cv.set(frame_map)
        return frame_map

class LocalTransaction:
    '''
    Coroutine local transaction, it depends on contextvars library which introduced in python 3.7
    Example usages:
    >>> async with local_transaction(pool, ...):
            await table('users').filter(...).update(...)
    '''
    _pool: Pool

    @staticmethod
    def get_conn(pool: Optional[Pool] = None) -> Optional[Connection]:
        '''
        try get connection from local contextvars, if the python version is too low, then return None

        :param pool: the db pool based with to look up a connection
        :returns: pushed connection if exists else None
        '''
        if pool is None:
            pool = get_default_pool()

        if contextvar_available():
            frame = _get_framemap().get_frame(pool)
            return frame.conn
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
        fmap = _get_framemap()

    async def __aenter__(self) -> Connection:
        frame = _get_framemap().get_frame(self._pool)
        if frame.conn is None:
            frame.conn_proxy = self._pool.acquire()
            frame.conn = await frame.conn_proxy.__aenter__()

        tx = frame.conn.transaction(**self.kwargs)
        await tx.__aenter__()
        frame.transactions.append(tx)
        return frame.conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        frame = _get_framemap().get_frame(self._pool)
        try:
            tx = frame.transactions.pop()
            await tx.__aexit__(exc_type, exc, tb)
        except IndexError:
            logging.error('index error on pop transactions from frame %s', frame)
            pass

        if not frame.transactions:
            if frame.conn_proxy:
                await frame.conn_proxy.__aexit__(exc_type, exc, tb)
                frame.conn_proxy = None
                frame.conn = None

        if exc_type is not None:
            logger.error('transaction error', exc_info=(exc_type, exc, tb))

local_transaction = LocalTransaction

