from typing import Any, Optional, List, Tuple, Union, TypeVar, Type, Dict, AsyncIterator

from collections.abc import ItemsView, KeysView, ValuesView
import asyncpg # type: ignore

from asyncpg.pool import Pool # type: ignore
from asyncpg import Record, Connection
from asyncpg.cursor import CursorFactory
from .db import local_transaction
from .expr import Expr, wrap, field, F
from .db import find_pool


SqlType = Tuple[str, List['Expr']]
FieldsType = Union[List[str], Tuple[str, ...]]

class DBRow:
    def __init__(self, row: Dict[str, Any]):
        self._row = row

    def __getattr__(self, key: str) -> Any:
        try:
            return self._row[key]
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(self.__class__.__name__, key))

    def __getitem__(self, key: str) -> Any:
        return self._row[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self._row.get(key, default)

    def __len__(self) -> int:
        return len(self._row)

    def __contains__(self, name) -> bool:
        return name in self._row

    def items(self) -> ItemsView:
        return self._row.items()

    def keys(self) -> KeysView:
        return self._row.keys()

    def values(self) -> ValuesView:
        return self._row.values()

    def __str__(self) -> str:
        return str(self._row)

    def __repr__(self) -> str:
        return repr(self._row)

T = TypeVar('T', bound='DBRow')


def table(name: str) -> 'Table':
    return Table(name)

class Join:
    def __init__(self, other_table_name: str, field1: str, field2: str, join_type: str='INNER'):
        self.other_table_name = other_table_name
        self.field1 = field1
        self.field2 = field2
        self.join_type = join_type

    def statement(self) -> str:
        return '{} JOIN {} ON {} = {}'.format(
            self.join_type,
            wrap(self.other_table_name),
            wrap(self.field1),
            wrap(self.field2))

class Table:
    name: str
    joins: List[Join]
    conn: Optional[Connection] = None
    pool: Union[Pool, str, None] = None

    def __init__(self, name: str):
        self.name = name
        self.joins = []
        self.conn = None
        self.pool = None

    def using(self, conn_or_pool: Union[Connection, Pool, str]) -> 'Table':
        t = Table(self.name)
        t.joins = self.joins[::]
        if isinstance(conn_or_pool, Connection):
            t.conn = conn_or_pool
        elif isinstance(conn_or_pool, (Pool, str)):
            t.pool = conn_or_pool
        elif conn_or_pool is not None:
            assert False, f'invalid conn type {conn_or_pool}'
        return t

    async def get_pool(self) -> Pool:
        if self.pool is None:
            return await find_pool('default')
        elif isinstance(self.pool, str):
            return await find_pool(self.pool)
        else:
            return self.pool

    def join(self, other: str, field1: str, field2: str) -> 'Table':
        t = Table(self.name)
        t.joins = self.joins + [Join(other, field1, field2)]
        return t

    def left_join(self, other: str, field1: str, field2: str) -> 'Table':
        t = Table(self.name)
        t.joins = self.joins + [Join(other, field1, field2, join_type='LEFT')]
        return t

    def right_join(self, other: str, field1: str, field2: str) -> 'Table':
        t = Table(self.name)
        t.joins = self.joins + [Join(other, field1, field2, join_type='RIGHT')]
        return t

    def filter(self, *args, **kw) -> 'Query':
        return Query(self).filter(*args, **kw)

    def exclude(self, *args, **kw) -> 'Query':
        return Query(self).exclude(*args, **kw)

    async def insert(self, **kw) -> 'Insert':
        assert kw
        return await Insert(self, kw).run()

    async def upsert(self, defaults=None, **kw) -> Tuple[Union['Insert', 'Update'], bool]:
        assert kw
        if defaults is None:
            defaults = {}

        obj = await self.filter(**kw).get_one()
        if not obj:
            values = defaults.copy()
            values.update(kw)
            return await self.insert(**values), True
        else:
            return await self.filter(
                **kw).update(**defaults), False

    async def get_or_insert(self, defaults=None, **kw) -> Tuple[Any, bool]:
        assert kw
        if defaults is None:
            defaults = {}

        r = await self.filter(**kw).get_one()
        if r:
            return r, False
        else:
            defaults.update(kw)
            return await self.insert(**defaults), True

    async def delete(self):
        return await Query(self).delete()

    async def select_for_update_skip_locked(self, *fields, **kw) -> List[Record]:
        kw['for_update'] = 'skip locked'
        return await self.select(*fields, **kw)

    async def select_for_update(self, *fields, **kw) -> List[Record]:
        kw['for_update'] = True
        return await self.select(*fields, **kw)

    async def select(self, *fields: str, **kw) -> List[Record]:
        return await Query(self).select(*fields, **kw)

    async def get_one(self, *fields: str) -> Optional[Record]:
        return await Query(self).get_one(*fields)

    async def get_all(self, *fields: str) -> List[Record]:
        return await Query(self).get_all(*fields)

    async def get_one_as(self, t: Type[T], *fields: str) -> Optional[T]:
        r = await self.get_one(*fields)
        if r is not None:
            return t(r)
        else:
            return None

    async def get_all_as(self, t: Type[T], *fields: str) -> List[T]:
        return [t(r) for r in await self.get_all(*fields)]

    def get_iter(self, *fields: str) -> AsyncIterator[Record]:
        return Query(self).get_iter(*fields)

    async def get_iter_as(self, t: Type[T], *fields: str) -> AsyncIterator[T]:
        async for r in self.get_iter(*fields):
            yield t(r)

    async def update(self, **kw):
        return await Query(self).update(**kw)

class Query:
    def __init__(self, table, **kw):
        self.table = table
        self.expr = kw.get('expr')
        self.offset_num = kw.get('offset_num')
        self.limiting = kw.get('limiting')
        self.ordering = kw.get('ordering')
        self.grouping = kw.get('grouping')

    def get_state(self):
        return {
            'expr': self.expr,
            'offset_num': self.offset_num,
            'limiting': self.limiting,
            'ordering': self.ordering,
            'grouping': self.grouping
            }

    def clone(self, **change):
        kw = self.get_state()
        kw.update(change)
        return Query(self.table, **kw)

    def filter(self, *args, **kw):
        tq = self.expr
        for q in list(args) + [
                Expr('=', field(k), v) for k, v in kw.items()]:
            if tq is not None:
                tq = Expr('and', tq, q)
            else:
                tq = q
        return self.clone(expr=tq)

    def orFilter(self, *args, **kw):
        tq = None
        for q in list(args) + [
                Expr('=', field(k), v) for k, v in kw.items()]:
            if tq is not None:
                tq = Expr('and', tq, q)
            else:
                tq = q
        if self.expr is None:
            return self.clone(expr=tq)
        else:
            return self.clone(expr=Expr('or', self.expr, tq))

    def exclude(self, *args, **kw):
        tq = None
        for q in list(args) + [
                Expr('=', field(k), v) for k, v in kw.items()]:
            if tq is not None:
                tq = Expr('and', tq, q)
            else:
                tq = q
        assert tq is not None
        if self.expr:
            ex = Expr('and', self.expr, Expr('not', tq, None))
        else:
            ex = Expr('not', tq, None)
        return self.clone(expr=ex)

    def offset(self, offset_num):
        assert offset_num >= 0
        return self.clone(offset_num=offset_num)

    def limit(self, limiting):
        assert limiting >= 0
        return self.clone(limiting=limiting)

    def order_by(self, *ordering):
        assert len(ordering) > 0
        return self.clone(ordering=ordering)

    def group_by(self, *grouping):
        assert len(grouping) > 0
        return self.clone(grouping=grouping)

    def get_order_sql(self):
        if self.ordering:
            orders = []
            for order in self.ordering:
                if order.startswith('-'):
                    orders.append('{} DESC'.format(wrap(order[1:])))
                else:
                    orders.append(wrap(order))
            return 'ORDER BY {}'.format(','.join(orders))

    def get_group_sql(self):
        if self.grouping:
            groups = [wrap(g) for g in self.grouping]
            return 'GROUP BY {}'.format(','.join(groups))

    async def select_for_update_skip_locked(self, *fields, **kw) -> List[Record]:
        kw['for_update'] = 'skip locked'
        return await self.select(*fields, **kw)

    async def select_for_update(self, *fields, **kw) -> List[Record]:
        kw['for_update'] = True
        return await self.select(*fields, **kw)

    async def select(self, *fields: str, **kw) -> List[Record]:
        if not fields:
            fields = ('*',)
        return await Select(self, fields, **kw).get_all()

    get_all = select

    async def get_one(self, *fields: str, **kw) -> Optional[Record]:
        if not fields:
            fields = ('*',)
        return await Select(self, fields, **kw).get_one()

    async def get_one_as(self, t: Type[T], *fields: str, **kw) -> Optional[T]:
        r = await self.get_one(*fields, **kw)
        if r is not None:
            return t(r)
        else:
            return None

    async def get_all_as(self, t: Type[T], *fields: str, **kw) -> List[T]:
        return [t(r) for r in await self.get_all(*fields, **kw)]

    def get_iter(self, *fields: str, **kw) -> AsyncIterator[Record]:
        if not fields:
            fields = ('*',)
        return Select(self, fields, **kw).get_iter()

    async def get_iter_as(self, t: Type[T], *fields: str, **kw) -> AsyncIterator[T]:
        async for r in self.get_iter(*fields, **kw):
            yield t(r)

    async def run(self) -> Any:
        return await self.select()

    async def update(self, **kw):
        assert kw
        return await Update(self, kw).run()

    async def delete(self):
        return await Delete(self).run()

    def get_condition_sql(self, params):
        if self.expr:
            return self.expr.get_sql_str(params)
        else:
            return Expr('value', True, None).get_sql_str(params)

class Action:
    def get_table(self) -> 'Table':
        raise NotImplemented

    def get_sql(self) -> SqlType:
        raise NotImplemented

    async def run_on_conn(self, conn: Connection,
                          stmt: str,
                          params: List[Expr],
                          return_one: bool=True) -> Any:
        if return_one:
            return await conn.fetchrow(stmt, *params)
        else:
            return await conn.fetch(stmt, *params)

    async def run(self, return_one=True):
        stmt, params = self.get_sql()
        table = self.get_table()
        if table.conn:
            return await self.run_on_conn(
                table.conn,
                stmt, params, return_one=return_one)
        else:
            pool = await table.get_pool()
            conn = await local_transaction.get_conn(pool=pool)
            if conn is not None:
                return await self.run_on_conn(
                    conn, stmt, params,
                    return_one=return_one)
            async with pool.acquire() as conn:
                return await self.run_on_conn(
                    conn, stmt, params,
                    return_one=return_one)

    def _run_on_conn_iter(self, conn: Connection,
                          stmt: str,
                          params: List[Expr]) -> CursorFactory:
        return conn.cursor(stmt, *params)

    async def run_iter(self) -> CursorFactory:
        stmt, params = self.get_sql()
        table = self.get_table()
        if table.conn:
            return self._run_on_conn_iter(
                table.conn,
                stmt, params)
        else:
            pool = await table.get_pool()
            conn = await local_transaction.get_conn(pool=pool)
            if conn is not None:
                return self._run_on_conn_iter(
                    conn, stmt, params)
            async with pool.acquire() as conn:
                return self._run_on_conn_iter(
                    conn, stmt, params)

class Select(Action):
    def __init__(self, query: 'Query', fields: FieldsType, for_update: Union[bool, str]=False, **kw):
        self.query = query
        self.fields = fields
        self.for_update = for_update

    def get_table(self) -> 'Table':
        return self.query.table

    def get_sql(self) -> SqlType:
        lines = [
            'SELECT {}'.format(
                ','.join([wrap(f) for f in self.fields])),
            ]

        lines.append(
            'FROM {}'.format(
                wrap(self.query.table.name)))

        if self.query.table.joins:
            for join in self.query.table.joins:
                lines.append(join.statement())

        params: List['Expr'] = []
        query_stmt = self.query.get_condition_sql(params)
        if query_stmt:
            lines.append('WHERE {}'.format(query_stmt))

        if self.query.grouping:
            lines.append(self.query.get_group_sql())

        if self.query.ordering:
            lines.append(self.query.get_order_sql())

        if self.query.limiting is not None:
            lines.append('LIMIT {}'.format(self.query.limiting))

        if self.query.offset_num is not None:
            lines.append('OFFSET {}'.format(self.query.offset_num))

        if self.for_update:
            if isinstance(self.for_update, str) and self.for_update.lower() == 'skip locked':
                lines.append('FOR UPDATE SKIP LOCKED')
            else:
                lines.append('FOR UPDATE')

        return '\n'.join(lines), params

    async def get_all(self) -> List[Record]:
        return await self.run(return_one=False)

    async def get_one(self) -> Optional[Record]:
        if self.query.limiting is None:
            # add limit 1, to reduce the results
            return await Select(
                self.query.limit(1),
                self.fields,
                self.for_update).get_one()
        return await self.run(return_one=True)

    async def get_one_as(self, t: Type[T]) -> Optional[T]:
        r = await self.get_one()
        if r is not None:
            return t(r)
        else:
            return None

    async def get_all_as(self, t: Type[T]) -> List[T]:
        return [t(r) for r in await self.get_all()]

    async def get_iter(self) -> AsyncIterator[Record]:
        async for r in await self.run_iter():
            yield r

    async def get_iter_as(self, t: Type[T]) -> AsyncIterator[T]:
        async for r in self.get_iter():
            yield t(r)

class Delete(Action):
    def __init__(self, query):
        self.query = query

    def get_table(self) -> 'Table':
        return self.query.table

    def get_sql(self) -> SqlType:
        params: List['Expr'] = []
        query_stmt = self.query.get_condition_sql(params)
        assert not self.query.table.joins
        lines = [
            'DELETE FROM {}'.format(wrap(self.query.table.name)),
            'WHERE {}'.format(query_stmt)
        ]
        return '\n'.join(lines), params

class Update(Action):
    def __init__(self, query, kw):
        self.query = query
        self.values = [Expr('=', field(k), Expr.parse(v))
                       for k, v in kw.items()]

    def get_table(self) -> 'Table':
        return self.query.table

    def get_value_sql(self, params: List['Expr']):
        arr = []
        for expr in self.values:
            arr.append(expr.get_sql_str(params))
        return ','.join(arr)

    def get_sql(self, returning: bool=True) -> SqlType:
        params: List['Expr'] = []
        set_stmt = self.get_value_sql(params)
        query_stmt = self.query.expr.get_sql_str(params)

        assert not self.query.table.joins
        lines = [
            'UPDATE {}'.format(wrap(self.query.table.name)),
            'SET {}'.format(set_stmt),
            'WHERE {}'.format(query_stmt),
            ]
        if returning:
            lines.append('RETURNING *')

        return '\n'.join(lines), params

class Insert(Action):
    table: Table
    fields: List[str]
    values: List['Expr']

    def __init__(self, table: 'Table', kw: Dict[str, Any]):
        self.table = table
        self.fields = []
        self.values = []
        for k, v in kw.items():
            self.fields.append(k)
            ve = Expr.parse(v)
            assert ve.op == 'value'
            self.values.append(ve)

    def get_table(self) -> 'Table':
        return self.table

    def get_value_sql(self, params: List[Any]) -> str:
        return ','.join(
            expr.get_sql_str(params)
            for expr in self.values)

    def get_sql(self) -> SqlType:
        params: List['Expr'] = []
        value_sql = self.get_value_sql(params)
        lines = [
            'INSERT INTO {}({})'.format(
                wrap(self.table.name),
                ','.join(
                    wrap(f) for f in self.fields)),
            'VALUES ({})'.format(value_sql),
            'RETURNING *'
            ]
        return '\n'.join(lines), params
