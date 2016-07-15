"""
This sub-module holds the AST (Abstract Syntax Tree) class definitions for representing SQL
commands and queries.
"""

from abc import abstractmethod, ABCMeta

import sql_dialects.dialects
from sql_dialects.enums import Nullary, Unary, Binary, Ternary, Aggregate, LiteralTypes, JoinTypes


__author__ = 'Aaron Hosford'


def same(expr1, expr2):
    if isinstance(expr1, SQLExpression):
        return expr1.same_as(expr2)
    elif isinstance(expr2, SQLExpression):
        return expr2.same_as(expr1)
    else:
        return expr1 == expr2


class SQLExpression(metaclass=ABCMeta):

    @abstractmethod
    def _get_repr_args(self):
        raise NotImplementedError()

    def copy(self):
        args = []
        kwargs = {}
        kw = False
        for name, value, default, kw_required in self._get_repr_args():
            if same(value, default):
                kw = True
            elif kw or kw_required:
                kwargs[name] = value
                kw = True
            else:
                args.append(value)
        return type(self)(*args, **kwargs)

    def __repr__(self):
        args = []
        kw = False
        for name, value, default, kw_required in self._get_repr_args():
            if same(value, default):
                kw = True
            elif kw or kw_required:
                args.append('%s=%r' % (name, value))
                kw = True
            else:
                args.append(repr(value))
        return type(self).__name__ + '(' + ', '.join(args) + ')'

    def same_as(self, other):
        return type(self) is type(other)


class SQLCommand(SQLExpression, metaclass=ABCMeta):

    def __init__(self, table=None, fields=None, where=None):
        assert table is None or (table and isinstance(table, TableExpression))
        assert fields is None or isinstance(fields, FieldList)
        assert where is None or isinstance(where, Where)

        self._table = table
        self._fields = fields
        self._where_clause = where

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('fields', self._fields, None, False),
            ('where', self._where_clause, None, False),
        ]

    def compile(self, dialect=None):
        dialect = sql_dialects.dialects.REGISTRY[dialect]
        return dialect.build_command(self)

    def __str__(self):
        # noinspection PyBroadException
        try:
            return self.compile()
        except Exception:
            return repr(self)

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, table):
        if isinstance(table, Identifier):
            table = Table(table)
        elif isinstance(table, str):
            table = Table(Identifier((table,)))
        else:
            assert isinstance(table, Table)
        assert table.identifier
        self._table = table

    @property
    def field_list(self):
        return self._fields

    @field_list.setter
    def field_list(self, fields):
        if fields is None or isinstance(fields, FieldList):
            self._fields = fields
            return

        fields = tuple(fields)

        if not fields:
            self._fields = None
            return

        field_list = []
        for field in fields:
            if isinstance(field, Alias):
                field_list.append(field)
            elif isinstance(field, Value):
                field_list.append(Alias(field))
            elif isinstance(field, str):
                field_list.append(Alias(Field(field)))
            else:
                value, alias = field
                field_list.append(Alias(value, alias))
        self._fields = FieldList(field_list)

    @property
    def where_clause(self):
        return self._where_clause

    @where_clause.setter
    def where_clause(self, clause):
        if isinstance(clause, Value):
            clause = Where(clause)
        else:
            assert isinstance(clause, Where)
        self._where_clause = clause

    def _from(self, table):
        assert self._table is None
        result = self.copy()
        result.table = table
        return result

    def fields(self, *fields):
        assert self._fields is None

        if len(fields) == 1 and (fields[0] is None or isinstance(fields[0], FieldList)):
            fields = fields[0]

        result = self.copy()
        result.field_list = fields
        return result

    def where(self, clause):
        assert self._where_clause is None
        result = self.copy()
        result.where_clause = clause
        return result

    def same_as(self, other):
        return (
            super().same_as(other) and
            same(self._table, other._table) and
            same(self._fields, other._fields)
        )


class SQLWriteCommand(SQLCommand):

    def __init__(self, table=None, fields=None, values=None, where=None):
        assert values is None or isinstance(values, (ValueList, Select))
        assert fields is None or isinstance(fields, FieldList)

        if fields is not None and values is not None:
            assert fields.width is None or values.width is None or fields.width == values.width

        super().__init__(table, fields, where)

        self._table = table
        self._fields = fields
        self._values = values
        self._where_clause = where

    @property
    def value_list(self):
        return self._values

    @value_list.setter
    def value_list(self, values):
        if values is None or isinstance(values, ValueList):
            self._values = values
            return

        values = tuple(values)

        if not values:
            self._values = None
            return

        value_list = []
        for value in values:
            if isinstance(value, Value):
                value_list.append(value)
            elif isinstance(value, (list, tuple)) and len(value) == 2:
                value, sql_type = value
                value_list.append(Literal(value, sql_type))
            else:
                value_list.append(Literal(value))
        self._values = ValueList(value_list)

    @property
    def width(self):
        if self._fields is not None:
            return self._fields.width
        if self._values is not None:
            return self._values.width
        return None

    def values(self, *values):
        assert self._values is None

        if len(values) == 1 and (values[0] is None or isinstance(values[0], ValueList)):
            values = values[0]

        result = self.copy()
        result.value_list = values
        return result

    def set(self, *args, **kwargs):
        # Two possible uses:
        #   update.set(field, value)
        #   update.set(field1=value1, field2=value2, ...)
        if args:
            assert not kwargs
            field, value = args
            pairs = {field: value}
        else:
            assert kwargs
            pairs = kwargs

        result = self.copy()

        for field, value in pairs.items():
            # Only original field names are allowed, not aliases, and they must belong to the table.
            if isinstance(field, Alias):
                assert field.name is None
                field = field.expression
            if result.table:
                field = result.table.get_field(field)
            else:
                field = Field(field)
            assert isinstance(field, Field)

            assert isinstance(value, Value)

            if result._fields is None:
                result._fields = FieldList([field])
                result._values = ValueList([value])
            else:
                result._fields.append(field)
                result._values.append(value)

        return result

    def same_as(self, other):
        return super().same_as(other) and same(self._values, other._values)


class Select(SQLCommand):

    def __init__(self, table=None, fields=None, where=None, distinct=False, limit=None,
                 order_by=None, group_by=None):
        assert isinstance(distinct, bool)
        assert limit is None or isinstance(limit, Limit)
        assert order_by is None or isinstance(order_by, OrderBy)
        assert group_by is None or isinstance(group_by, GroupBy)

        super().__init__(table, fields, where)

        self._distinct = distinct
        self._limited_to = limit
        self._order = order_by
        self._grouping = group_by

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('fields', self._fields, None, False),
            ('where', self._where_clause, None, False),
            ('distinct', self._distinct, False, False),
            ('limit', self._limited_to, None, False),
            ('order_by', self._order, None, False),
            ('group_by', self._grouping, None, False),
        ]

    @property
    def is_distinct(self):
        return self._distinct

    @is_distinct.setter
    def is_distinct(self, distinct):
        assert isinstance(distinct, bool)
        self._distinct = distinct

    @property
    def limited_to(self):
        return self._limited_to

    @limited_to.setter
    def limited_to(self, limit):
        if isinstance(limit, int):
            limit = Limit(limit)
        else:
            assert isinstance(limit, Limit)
        self._limited_to = limit

    @property
    def order(self):
        return self._order

    @order.setter
    def order(self, order):
        if isinstance(order, OrderBy):
            self._order = order
        else:
            self._order = OrderBy(order)

    @property
    def grouping(self):
        return self._grouping

    @grouping.setter
    def grouping(self, grouping):
        if isinstance(grouping, GroupBy):
            self._grouping = grouping
        else:
            self._grouping = GroupBy(grouping)

    @property
    def width(self):
        if self._fields is None:
            return None
        return self._fields.width

    def from_(self, table):
        return self._from(table)

    def distinct(self):
        result = self.copy()
        result.is_distinct = True
        return result

    def limit(self, limit):
        assert self._limited_to is None
        result = self.copy()
        result.limited_to = limit
        return result

    def order_by(self, *order):
        assert self._order is None

        if len(order) == 1 and isinstance(order[0], OrderBy):
            order = order[0]

        result = self.copy()
        result.order = order
        return result

    def group_by(self, *grouping):
        assert self._grouping is None

        if len(grouping) == 1 and isinstance(grouping[0], OrderBy):
            grouping = grouping[0]

        result = self.copy()
        result.grouping = grouping
        return result

    def same_as(self, other):
        return (
            super().same_as(other) and
            same(self._distinct, other._distinct) and
            same(self._limited_to, other._limit_to) and
            same(self._order, other._ordering) and
            same(self._grouping, other._grouping)
        )


class Insert(SQLWriteCommand):

    def __init__(self, table=None, fields=None, values=None):
        super().__init__(table, fields, values, where=None)

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('fields', self._fields, None, False),
            ('values', self._values, None, False),
        ]

    def into(self, table):
        return self._from(table)

    def where(self, clause):
        raise NotImplementedError("WHERE clauses are not supported for INSERT statements.")


class Update(SQLWriteCommand):

    def __init__(self, table=None, fields=None, values=None, where=None):
        assert not isinstance(values, Select)
        super().__init__(table, fields, values, where)

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('fields', self._fields, None, False),
            ('values', self._values, None, False),
            ('where', self._where_clause, None, False),
        ]

    def from_(self, table):
        return self._from(table)


class Delete(SQLCommand):

    def __init__(self, table=None, where=None):
        super().__init__(table, fields=None, where=where)

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('where', self._where_clause, None, False),
        ]

    def from_(self, table):
        return self._from(table)

    def fields(self, *fields):
        raise NotImplementedError("Field lists are not supported for DELETE statements.")


class Identifier(SQLExpression):

    def __init__(self, path):
        assert all(element and isinstance(element, str) for element in path)

        super().__init__()

        self._path = path

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('path', self._path, NotImplemented, False),
        ]

    @property
    def path(self):
        return self._path

    @property
    def name(self):
        return self._path[-1]

    @property
    def parent(self):
        if len(self._path) > 1:
            return Identifier(self._path[:-1])
        else:
            return None

    def get(self, name):
        if isinstance(name, str):
            return Identifier(self._path + (name,))
        else:
            assert isinstance(name, Identifier)
            return Identifier(self._path + name._path)

    def __bool__(self):
        return bool(self._path)

    def __getattr__(self, name):
        return self.get(name)

    def __getitem__(self, name):
        return self.get(name)

    def same_as(self, other):
        return super().same_as(other) and self._path == other._path


class TableExpression(SQLExpression, metaclass=ABCMeta):
    """
    Base class for table expressions.
    """

    @abstractmethod
    @property
    def leftmost(self):
        raise NotImplementedError()

    def join(self, table, on=None):
        return Join(JoinTypes.INNER, self, table, on)

    def inner_join(self, table, on=None):
        return Join(JoinTypes.INNER, self, table, on)

    def left_join(self, table, on=None):
        return Join(JoinTypes.LEFT, self, table, on)

    def right_join(self, table, on=None):
        return Join(JoinTypes.RIGHT, self, table, on)

    def outer_join(self, table, on=None):
        return Join(JoinTypes.OUTER, self, table, on)

    def full_join(self, table, on=None):
        return Join(JoinTypes.OUTER, self, table, on)

    def select(self, *fields):
        return Select(self).fields(*fields)

    def update(self, *fields):
        return Update(self).fields(*fields)

    def insert(self, *fields):
        return Insert(self).fields(*fields)

    def delete(self):
        return Delete(self)

    @abstractmethod
    def get_field(self, field):
        raise NotImplementedError()


class Table(TableExpression):

    def __init__(self, identifier):
        if identifier is None:
            identifier = Identifier(())
        elif isinstance(identifier, tuple):
            identifier = Identifier(identifier)
        else:
            assert isinstance(identifier, Identifier)

        super().__init__()

        self._identifier = identifier

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('identifier', self._identifier, NotImplemented, False),
        ]

    def __repr__(self):
        result = 'T'
        for element in self._identifier.path:
            if element.isidentifier():
                result += '.' + element
            else:
                result += '[%r]' % element
        return result

    @property
    def identifier(self):
        return self._identifier

    @property
    def leftmost(self):
        return self

    def get_field(self, field):
        if isinstance(field, Alias):
            alias, field = field, field.expression
        else:
            alias = None
        if isinstance(field, Field):
            if not field.identifier.parent:
                # It's just a name; give it a full path.
                field = Field(self._identifier[field.identifier.name])
                if alias:
                    alias = Alias(field, alias.name)
            else:
                assert same(field.identifier.parent, self.identifier)
            return alias or field
        else:
            assert isinstance(field, str)
            return Field(self._identifier[field])

    def __getitem__(self, name):
        if isinstance(name, str):
            path = (name,)
        else:
            path = name
        assert path and isinstance(path, tuple)
        return Table(self._identifier.path + path)

    def __getattr__(self, name):
        return Table(self._identifier[name])

    @property
    def field(self):
        return Field(self._identifier)

    def same_as(self, other):
        return super().same_as(other) and same(self._identifier, other._identifier)


class Join(TableExpression):

    def __init__(self, join_type, left, right, on=None):
        assert JoinTypes.is_valid(join_type)
        assert isinstance(left, TableExpression)
        assert isinstance(right, Table), "Right argument must be table. JOINS are left-associative."
        assert on is None or isinstance(on, On)

        super().__init__()

        self._join_type = join_type
        self._left = left
        self._right = right
        self._on = on

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('join_type', self._join_type, NotImplemented, False),
            ('left', self._left, NotImplemented, False),
            ('right', self._right, NotImplemented, False),
            ('on', self._on, None, False),
        ]

    @property
    def join_type(self):
        return self._join_type

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    @property
    def on(self):
        return self._on

    def leftmost(self):
        current = self._left
        while isinstance(self._left, Join):
            current = current._left
        assert isinstance(current, Table)
        return current

    def get_field(self, field):
        if isinstance(field, Alias):
            alias, field = field, field.expression
        else:
            alias = None
        assert isinstance(field, Field)
        if field.table is None:
            return alias or field  # We can't set the parent automatically.
        if field.table is self._right:
            return alias or field
        else:
            return self._left.get_field(alias or field)

    def same_as(self, other):
        return (
            super().same_as(other) and
            self._join_type == other._join_type and
            same(self._left, other._left) and
            same(other._right, self._right) and
            same(self._on, other._on)
        )


class Alias(SQLExpression):

    def __init__(self, expression, alias=None):
        assert isinstance(expression, Value)
        assert alias is None or (alias and isinstance(alias, str))

        self._expression = expression
        self._alias = alias

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('expression', self._expression, NotImplemented, False),
            ('alias', self._alias, None, False),
        ]

    @property
    def name(self):
        return self._alias

    @property
    def expression(self):
        return self._expression

    def same_as(self, other):
        return (
            super().same_as(other) and
            same(self._expression, other._expression) and
            same(self._alias, other._alias)
        )


class FieldList(SQLExpression):
    """
    A list of field_list in a SQL command.
    """

    def __init__(self, entries):
        assert entries

        aliases = []
        for entry in entries:
            if isinstance(entry, Alias):
                aliases.append(entry)
            elif isinstance(entry, str):
                aliases.append(Alias(Field(Identifier((entry,)))))
            elif isinstance(entry, Identifier):
                aliases.append(Alias(Field(entry)))
            else:
                assert isinstance(entry, Field)
                aliases.append(Alias(entry))

        super().__init__()

        self._entries = tuple(aliases)

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('entries', self._entries, NotImplemented, False),
        ]

    @property
    def entries(self):
        return self._entries

    @property
    def width(self):
        return len(self._entries)

    def append(self, entry):
        if isinstance(entry, Alias):
            alias = entry
        else:
            assert isinstance(entry, Field)
            alias = Alias(entry)

        self._entries += (alias,)

    def same_as(self, other):
        return (
            super().same_as(other) and
            self.width == other.width and
            all(same(mine, yours) for mine, yours in zip(self._entries, other._entries))
        )


class ValueList(SQLExpression):
    """
    A list of value_list in a SQL command.
    """

    def __init__(self, entries):
        entries = tuple(entries)
        assert all(isinstance(entry, Value) for entry in entries)

        super().__init__()

        self._entries = entries

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('entries', self._entries, NotImplemented, False),
        ]

    @property
    def entries(self):
        return self._entries

    @property
    def width(self):
        return len(self._entries)

    def append(self, value):
        assert isinstance(value, Value)
        self._entries += (value,)

    def same_as(self, other):
        return (
            super().same_as(other) and
            self.width == other.width and
            all(same(mine, yours) for mine, yours in zip(self._entries, other._entries))
        )


class Limit(SQLExpression):
    """
    The maximum number of rows to yield in a query.
    """

    def __init__(self, count):
        assert isinstance(count, int)
        assert count > 0

        super().__init__()

        self._count = count

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('count', self._count, NotImplemented, False),
        ]

    @property
    def count(self):
        return self._count

    def same_as(self, other):
        return super().same_as(other) and self._count == other._count


class On(SQLExpression):
    """
    A SQL on clause.
    """

    def __init__(self, condition):
        assert isinstance(condition, Value)

        super().__init__()

        self._condition = condition

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('condition', self._condition, NotImplemented, False),
        ]

    @property
    def condition(self):
        return self._condition

    def same_as(self, other):
        return super().same_as(other) and same(self._condition, other._condition)


class Where(SQLExpression):
    """
    A SQL where_clause clause.
    """

    def __init__(self, condition):
        assert isinstance(condition, Value)

        super().__init__()

        self._condition = condition

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('condition', self._condition, NotImplemented, False),
        ]

    @property
    def condition(self):
        return self._condition

    def same_as(self, other):
        return super().same_as(other) and same(self._condition, other._condition)


class GroupBy(SQLExpression):
    """
    A SQL group by clause.
    """

    def __init__(self, fields):
        fields = tuple(fields)
        assert fields
        assert all(isinstance(field, Field) for field in fields)

        super().__init__()

        self._fields = fields

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('fields', self._fields, NotImplemented, False),
        ]

    @property
    def fields(self):
        return self._fields

    def same_as(self, other):
        return (
            super().same_as(other) and
            len(self._fields) == len(other._fields) and
            all(same(mine, yours) for mine, yours in zip(self._fields, other._fields))
        )


class OrderByEntry(SQLExpression):
    """
    An entry in a SQL order by clause.
    """

    def __init__(self, field, ascending=True):
        assert isinstance(field, Field)
        assert isinstance(ascending, bool)

        super().__init__()

        self._field = field
        self._ascending = ascending

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('field', self._field, NotImplemented, False),
            ('ascending', self._ascending, True, False),
        ]

    @property
    def field(self):
        return self._field

    @property
    def ascending(self):
        return self._ascending

    def same_as(self, other):
        return (
            super().same_as(other) and
            same(self._field, other._field) and
            self._ascending == other._ascending
        )


class OrderBy(SQLExpression):
    """
    A SQL order by clause.
    """

    def __init__(self, entries):
        entries = tuple(entries)
        assert entries
        assert all(isinstance(entry, OrderByEntry) for entry in entries)

        super().__init__()

        self._entries = entries

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('entries', self._entries, NotImplemented, False),
        ]

    @property
    def entries(self):
        return self._entries

    def same_as(self, other):
        return (
            super().same_as(other) and
            len(self._entries) == len(other._entries) and
            all(same(mine, yours) for mine, yours in zip(self._entries, other._entries))
        )


class Value(SQLExpression, metaclass=ABCMeta):
    """
    Base class for SQL expressions that represent values, e.g. field_list, parameters, literals,
    and the application of operators or functions to other value expressions.
    """

    def as_(self, alias):
        return Alias(self, alias)

    def __eq__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.EQ, self, other)

    def __ne__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.NE, self, other)

    def __lt__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.LT, self, other)

    def __gt__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.GT, self, other)

    def __ge__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.LE, self, other)

    def __le__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.GE, self, other)

    def __invert__(self):
        return Operation(Unary.NOT, self)

    def __and__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.AND, self, other)

    def __rand__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.AND, other, self)

    def __or__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.OR, self, other)

    def __ror__(self, other):
        if not isinstance(other, Value):
            other = Literal(other)
        return Operation(Binary.OR, other, self)

    def __neg__(self):
        return Operation(Unary.NEG, self)

    def length(self):
        return Operation(Unary.LENGTH, self)

    def upper(self):
        return Operation(Unary.UPPER_CASE, self)

    def lower(self):
        return Operation(Unary.LOWER_CASE, self)

    def round(self, precision):
        if not isinstance(precision, Value):
            precision = Literal(precision)
        return Operation(Binary.ROUND, self, precision)

    def format(self, form):
        if not isinstance(form, Value):
            form = Literal(form)
        return Operation(Binary.FORMAT, self, form)

    def substring(self, start, length):
        if not isinstance(start, Value):
            start = Literal(start)
        if not isinstance(length, Value):
            length = Literal(length)
        return Operation(Ternary.SUBSTRING, self, start, length)

    def if_else(self, condition, alternative):
        assert isinstance(condition, Value), "Condition cannot be a literal value"
        if not isinstance(alternative, Value):
            alternative = Literal(alternative)
        return Operation(Ternary.IF_ELSE, condition, self, alternative)


class Field(Value):
    """
    A field in a table or view.
    """

    def __init__(self, identifier):
        if identifier is None:
            identifier = Identifier(())
        elif isinstance(identifier, tuple):
            identifier = Identifier(identifier)
        elif isinstance(identifier, str):
            identifier = Identifier((identifier,))
        else:
            assert isinstance(identifier, Identifier)

        super().__init__()

        self._identifier = identifier

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('identifier', self._identifier, NotImplemented, False),
        ]

    def __repr__(self):
        result = 'F'
        for element in self._identifier.path:
            if element.isidentifier():
                result += '.' + element
            else:
                result += '[%r]' % element
        return result

    @property
    def identifier(self):
        return self._identifier

    @property
    def table(self):
        if self._identifier.parent:
            return Table(self._identifier.parent)
        else:
            return None

    def __getitem__(self, name):
        if isinstance(name, str):
            path = (name,)
        else:
            path = name
        assert path and isinstance(path, tuple)
        return Field(self._identifier.path + path)

    def __getattr__(self, name):
        return Field(self._identifier[name])

    def same_as(self, other):
        return super().same_as(other) and same(self._identifier, other._identifier)


class Parameter(Value):
    """
    A parameter value provided externally to the SQL command.
    """

    def __init__(self, name=None):
        assert name is None or (name and isinstance(name, str))

        super().__init__()

        self._name = name

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('name', self._name, None, False),
        ]

    @property
    def name(self):
        return self._name

    def same_as(self, other):
        return self is other


class Literal(Value):
    """
    A literal value, such as a string, integer, or date.
    """

    def __init__(self, value, sql_type=None):
        if sql_type is None:
            sql_type = LiteralTypes.get_sql_type(type(value))
        else:
            assert LiteralTypes.is_valid(sql_type)
            assert isinstance(value, LiteralTypes.get_python_type(sql_type))

        self._value = value
        self._sql_type = sql_type

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('value', self._value, NotImplemented, False),
            ('sql_type', self._sql_type, None, False),
        ]

    @property
    def value(self):
        return self._value

    @property
    def sql_type(self):
        return self._sql_type

    def same_as(self, other):
        return (
            super().same_as(other) and
            self._value == other._value and
            self._sql_type == other._sql_type
        )


class Operation(Value):
    """
    A value constructed through the application of an operator or function.
    """

    def __init__(self, operator, *operands):
        arity = len(operands)
        if not arity:
            assert Nullary.is_valid(operator), "Wrong number of operands."
        elif arity == 1:
            assert (Unary.is_valid(operator) or Aggregate.is_valid(operator)), \
                "Wrong number of operands."
        elif arity == 2:
            assert Binary.is_valid(operator), "Wrong number of operands."
        else:
            assert arity == 3, "Too many operands."
            assert Ternary.is_valid(operator), "Wrong number of operands."

        assert all(isinstance(operand, Value) for operand in operands)

        super().__init__()

        self._operator = operator
        self._operands = operands

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('operator', self._operator, NotImplemented, False),
        ] + [('*operands', operand, NotImplemented, False) for operand in self._operands]

    @property
    def operator(self):
        return self._operator

    @property
    def operands(self):
        return self._operands

    def same_as(self, other):
        return (
            super().same_as(other) and
            self._operator == other._operator and
            len(self._operands) == len(other._operands) and
            all(same(mine, yours) for mine, yours in zip(self._operands, other._operands))
        )


class QueryValue(Value):

    def __init__(self, query):
        assert isinstance(query, Select)
        assert query.field_list.width == 1

        super().__init__()

        self._query = query

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('query', self._query, NotImplemented, False),
        ]

    @property
    def query(self):
        return self._query

    def same_as(self, other):
        return super().same_as(other) and same(self._query, other._query)


def select(*fields):
    if fields:
        return Select().fields(*fields)
    else:
        return Select()


def update(*fields, **field_value_pairs):
    assert not fields or not field_value_pairs
    if fields:
        return Update().fields(*fields)
    elif field_value_pairs:
        return Update().set(**field_value_pairs)
    else:
        return Update()


def insert(*fields, **field_value_pairs):
    assert not fields or not field_value_pairs
    if fields:
        return Insert().fields(*fields)
    elif field_value_pairs:
        return Insert().set(**field_value_pairs)
    else:
        return Insert()


def delete():
    return Delete()
