from typing import Any, List

import re

id_pattern = re.compile(r'\w+$')

def wrap(name: str) -> str:
    arr = []
    for term in name.split('.'):
        if id_pattern.match(term):
            arr.append('"{}"'.format(term))
        else:
            arr.append(term)
    return '.'.join(arr)

class Expr:
    op: str
    left: Any
    right: Any

    @classmethod
    def parse(cls, value: Any) -> 'Expr':
        if isinstance(value, Expr):
            return value
        else:
            return Expr('value', value, None)

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
        return Expr('in', self.parse(value), self.left)

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

    def get_sql_str(self, params: List['Expr']) -> str:
        if self.op == 'value':
            params.append(self.left)
            return f'${len(params)}'
        elif self.op == 'field':
            return wrap(self.left)
        elif self.op == 'safe':
            return self.left
        elif self.op == 'neg':
            return '-{}'.format(
                self.left.get_sql_str(params))
        elif self.op == 'not':
            return 'not ({})'.format(
                self.left.get_sql_str(params))
        elif self.op == 'in':
            left_stmt = self.left.get_sql_str(params)
            places = []
            assert isinstance(self.right, (tuple, list))
            for v in self.right:
                params.append(v)
                places.append('${}'.format(len(params)))

            return '{} in ({})'.format(
                left_stmt, ','.join(places))
        elif self.op == 'not in':
            left_stmt = self.left.get_sql_str(params)
            places = []
            assert isinstance(self.right, (tuple, list))
            for v in self.right:
                params.append(v)
                places.append('${}'.format(len(params)))

            return '{} not in ({})'.format(
                left_stmt, ','.join(places))
        elif self.op == '=' and self.right is None:
            left_stmt = self.left.get_sql_str(params)
            if self.left.is_binop():
                left_stmt = '({})'.format(left_stmt)
            return '{} is null'.format(left_stmt)
        elif self.op == '<>' and self.right is None:
            left_stmt = self.left.get_sql_str(params)
            if self.left.is_binop():
                left_stmt = '({})'.format(left_stmt)
            return '{} is not null'.format(left_stmt)
        else:
            left_stmt = self.left.get_sql_str(params)
            right = self.parse(self.right)
            right_stmt = right.get_sql_str(params)
            if self.is_binop():
                if self.left.is_binop():
                    left_stmt = '({})'.format(left_stmt)
                if right.is_binop():
                    right_stmt = '({})'.format(right_stmt)
            return '{} {} {}'.format(
                left_stmt, self.op, right_stmt)

def field(name: str) -> 'Expr':
    return Expr('field', name, None)

F = field

def safe(name: str) -> 'Expr':
    return Expr('safe', name, None)

def list_expr(*values) -> 'Expr':
    return Expr('list', values, None)
