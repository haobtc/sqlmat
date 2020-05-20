from typing import Any, Optional, List, Tuple, Union
import re
import asyncpg

id_pattern = re.compile(r'\w+$')

def wrap(name: str) -> str:
    arr = []
    for term in name.split('.'):
        if id_pattern.match(term):
            arr.append('"{}"'.format(term))
        else:
            arr.append(term)
    return '.'.join(arr)

_pool = None
def set_default_pool(pool):
    global _pool
    _pool = pool

class Expr:
    op: str
    left: Any
    right: Any

    @classmethod
    def parse(cls, v: Any) -> 'Expr':
        if isinstance(v, Expr):
            return v
        else:
            return Expr('value', v, None)

    def __str__(self) -> str:
        return f'({self.op} {self.left} {self.right})'

    def __init__(self, op: str, left: Any, right: Any):
        self.op = op
        self.left = left
        self.right = right

    def __eq__(self, other: Any) -> 'Expr': # type: ignore
        return Expr('=', self, other)

    def __ne__(self, other: Any) -> 'Expr': # type: ignore
        return Expr('<>', self, other)

    def __lt__(self, other: Any) -> 'Expr':
        return Expr('<', self, other)

    def __le__(self, other: Any) -> 'Expr':
        return Expr('<=', self, other)

    def __gt__(self, other: Any) -> 'Expr':
        return Expr('>', self, other)

    def __ge__(self, other: Any) -> 'Expr':
        return Expr('>=', self, other)

    def __or__(self, other: Any) -> 'Expr':
        return Expr('or', self, other)

    def __and__(self, other: Any) -> 'Expr':
        return Expr('and', self, other)

    def __neg__(self) -> 'Expr':
        return Expr('neg', self, None)

    def __add__(self, other: Any) -> 'Expr':
        return Expr('+', self, other)

    def __sub__(self, other: Any) -> 'Expr':
        return Expr('-', self, other)

    def __mul__(self, other: Any) -> 'Expr':
        return Expr('*', self, other)

    def __div__(self, other: Any) -> 'Expr':
        return Expr('/', self, other)

    def __contains__(self, value: Any) -> 'Expr':
        return Expr('in', self.parse(value) , self.left)

    def _in(self, *alist: Any) -> 'Expr':
        assert alist
        list_expr = Expr('list', alist, None)
        return Expr('in', self, list_expr.left)

    def like(self, pattern: str) -> 'Expr':
        return Expr('like', self, pattern)

    def ilike(self, pattern: str) -> 'Expr':
        return Expr('ilike', self, pattern)

    def startswith(self, prefix: str) -> 'Expr':
        return self.like('{}%'.format(prefix))

    def is_null(self) -> 'Expr':
        return Expr('=', self, None)

    def is_not_null(self) -> 'Expr':
        return Expr('<>', self, None)

    def _not(self) -> 'Expr':
        return Expr('not', self, None)

    def not_in(self, *alist) -> 'Expr':
        assert alist
        list_expr = Expr('list', alist, None)
        return Expr('not in', self, list_expr.left)

    def is_binop(self) -> bool:
        return self.op in ('+', '-', '*', '/', '^', 'like', 'ilike')

    def get_sql(self, params) -> str:
        if self.op == 'value':
            params.append(self.left)
            return '${}'.format(len(params))
        elif self.op == 'field':
            return wrap(self.left)
        elif self.op == 'safe':
            return self.left
        elif self.op == 'neg':
            return '-{}'.format(
                self.left.get_sql(params))
        elif self.op == 'not':
            return 'not ({})'.format(
                self.left.get_sql(params))
        elif self.op == 'in':
            left_stmt = self.left.get_sql(params)
            places = []
            assert isinstance(self.right, (tuple, list))
            for v in self.right:
                params.append(v)
                places.append('${}'.format(len(params)))

            return '{} in ({})'.format(
                left_stmt, ','.join(places))
        elif self.op == 'not in':
            left_stmt = self.left.get_sql(params)
            places = []
            assert isinstance(self.right, (tuple, list))
            for v in self.right:
                params.append(v)
                places.append('${}'.format(len(params)))

            return '{} not in ({})'.format(
                left_stmt, ','.join(places))
        elif self.op == '=' and self.right is None:
            left_stmt = self.left.get_sql(params)
            if self.left.is_binop():
                left_stmt = '({})'.format(left_stmt)
            return '{} is null'.format(left_stmt)
        elif self.op == '<>' and self.right is None:
            left_stmt = self.left.get_sql(params)
            if self.left.is_binop():
                left_stmt = '({})'.format(left_stmt)
            return '{} is not null'.format(left_stmt)
        else:
            left_stmt = self.left.get_sql(params)
            right = self.parse(self.right)
            right_stmt = right.get_sql(params)
            if self.is_binop():
                if self.left.is_binop():
                    left_stmt =  '({})'.format(left_stmt)
                if right.is_binop():
                    right_stmt = '({})'.format(right_stmt)
            return '{} {} {}'.format(
                left_stmt, self.op, right_stmt)

def field(name):
    return Expr('field', name, None)

F = field

def safe(name):
    return Expr('safe', name, None)

def list_expr(*values):
    return Expr('list', values, None)

def table(name):
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
    def __init__(self, name: str):
        self.name = name
        self.joins = []
        self.conn = None

    def using(self, conn) -> 'Table':
        t = Table(self.name)
        t.joins = self.joins[::]
        t.conn = conn
        return t

    async def get_pool(self):
        assert _pool
        return _pool

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

    async def upsert(self, defaults=None, **kw) -> Tuple[Union[Insert, Update], bool]:
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

    async def select(self, *fields, **kw):
        return await Query(self).select(*fields, **kw)

    async def get_one(self, *fields):
        return await Query(self).get_one(*fields)

    async def get_all(self, *fields):
        return await Query(self).get_all(*fields)

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

    async def select(self, *fields, **kw) -> Any:
        if not fields:
            fields = ('*',)
        return await Select(self, fields, **kw).get_all()

    get_all = select

    async def get_one(self, *fields, **kw):
        if not fields:
            fields = ['*']
        return await Select(self, fields, **kw).get_one()

    async def run(self) -> Any:
        return await self.select()

    async def update(self, **kw):
        assert kw
        return await Update(self, kw).run()

    async def delete(self):
        return await Delete(self).run()

    def get_condition_sql(self, params):
        if self.expr:
            return self.expr.get_sql(params)
        else:
            return Expr('value', True, None).get_sql(params)

class Action:
    def get_table(self):
        raise NotImplemented

    async def run(self, return_one=True):
        stmt, params = self.get_sql()
        table = self.get_table()
        if table.conn:
            if return_one:
                return await table.conn.fetchrow(stmt, *params)
            else:
                return await table.conn.fetch(stmt, *params)
        else:
            pool = await table.get_pool()
            async with pool.acquire() as conn:
                if return_one:
                    return await conn.fetchrow(stmt, *params)
                else:
                    return await conn.fetch(stmt, *params)

class Select(Action):
    def __init__(self, query, fields, for_update=False, **kw):
        self.query = query
        self.fields = fields
        self.for_update = for_update

    def get_table(self):
        return self.query.table

    def get_sql(self):
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

        params = []
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
            lines.append('FOR UPDATE')

        return '\n'.join(lines), params

    async def get_all(self):
        return await self.run(return_one=False)

    async def get_one(self):
        if self.query.limiting is None:
            # add limit 1, to reduce the results
            return await Select(
                self.query.limit(1),
                self.fields,
                self.for_update).get_one()
        return await self.run(return_one=True)

class Delete(Action):
    def __init__(self, query):
        self.query = query

    def get_table(self):
        return self.query.table

    def get_sql(self):
        params = []
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

    def get_table(self):
        return self.query.table

    def get_value_sql(self, params):
        arr = []
        for expr in self.values:
            arr.append(expr.get_sql(params))
        return ','.join(arr)

    def get_sql(self, returning=True) -> Tuple[str, List['Expr']]:
        params: List['Expr'] = []
        set_stmt = self.get_value_sql(params)
        query_stmt = self.query.expr.get_sql(params)

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

    def __init__(self, table: 'Table', kw):
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
            expr.get_sql(params)
            for expr in self.values)

    def get_sql(self) -> Tuple[str, List['Expr']]:
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
