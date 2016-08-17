"""
This sub-module holds the AST (Abstract Syntax Tree) class definitions for representing SQL
commands and queries.
"""

from abc import abstractmethod, ABCMeta

import sql_dialects.dialects
from sql_dialects.enums import Nullary, Unary, Binary, Ternary, Aggregate, LiteralTypes, JoinTypes


__author__ = 'Aaron Hosford'


def same(expr1, expr2):
    """Determine whether two ASTs are the same. This function is necessary for *testing* equality, because
    the == operator has been overridden for *building* equality comparison ASTs."""
    if isinstance(expr1, SQLExpression):
        return expr1.same_as(expr2)
    elif isinstance(expr2, SQLExpression):
        return expr2.same_as(expr1)
    else:
        return expr1 == expr2


class SQLExpression(metaclass=ABCMeta):
    """Abstract base class for all SQL expressions and statements."""

    @abstractmethod
    def _get_repr_args(self):
        raise NotImplementedError()

    def copy(self):
        """Make a copy of this SQL expression."""
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
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        return type(self) is type(other)


class SQLCommand(SQLExpression, metaclass=ABCMeta):
    """Abstract base class for SQL commands, i.e. selects, inserts, updates, and deletes."""

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
        """Compile this SQL command into the given (or current default) dialect."""
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
        """The table or JOIN that this SQL command operates on."""
        return self._table

    @table.setter
    def table(self, table):
        """The table or JOIN that this SQL command operates on."""
        if isinstance(table, Identifier):
            table = Table(table)
        elif isinstance(table, str):
            table = Table(Identifier((table,)))
        else:
            assert isinstance(table, TableExpression)
        if isinstance(table, Table):
            assert table.identifier
        self._table = table

    @property
    def field_list(self):
        """The list of fields for this SQL command."""
        return self._fields

    @field_list.setter
    def field_list(self, fields):
        """The list of fields for this SQL command."""
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
        """The WHERE clause for this SQL command."""
        return self._where_clause

    @where_clause.setter
    def where_clause(self, clause):
        """The WHERE clause for this SQL command."""
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
        """Create a new SQL command object by adding a list of fields."""
        assert self._fields is None

        if len(fields) == 1 and (fields[0] is None or isinstance(fields[0], FieldList)):
            fields = fields[0]

        result = self.copy()
        result.field_list = fields
        return result

    def join(self, table):
        """Create a new SQL command object by joining the table to another one."""
        left = self._table
        assert isinstance(left, TableExpression)
        result = self.copy()
        result.table = left.join(table)
        return result

    def on(self, clause):
        """Create a new SQL command object by adding an ON clause to the join."""
        join = self._table
        assert isinstance(join, Join)
        result = self.copy()
        result.table = join.on(clause)
        return result

    def where(self, clause):
        """Create a new SQL command object by adding a where clause."""
        assert self._where_clause is None
        result = self.copy()
        result.where_clause = clause
        return result

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, SQLCommand)
        return (
            same(self._table, other._table) and
            same(self._fields, other._fields)
        )


class SQLWriteCommand(SQLCommand):
    """Abstract base class for SQL commands that write new data to the database, i.e. inserts and updates."""

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
        """The list of values to be written to the fields."""
        return self._values

    @value_list.setter
    def value_list(self, values):
        """The list of values to be written to the fields."""
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
        """The number of field/value pairs in this SQL insert or update."""
        if self._fields is not None:
            return self._fields.width
        if self._values is not None:
            return self._values.width
        return None

    def values(self, *values):
        """Create a new SQLWriteCommand instance by adding a list of values."""
        assert self._values is None

        if len(values) == 1 and (values[0] is None or isinstance(values[0], ValueList)):
            values = values[0]

        result = self.copy()
        result.value_list = values
        return result

    def set(self, *args, **kwargs):
        """Create a new SQLWriteCommand instance by adding one or more field/value pairs."""
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
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, SQLWriteCommand)
        return same(self._values, other._values)


class Select(SQLCommand):
    """A SQL select statement, as an AST."""

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
        """Whether the select statement queries for distinct records."""
        return self._distinct

    @is_distinct.setter
    def is_distinct(self, distinct):
        """Whether the select statement queries for distinct records."""
        assert isinstance(distinct, bool)
        self._distinct = distinct

    @property
    def limited_to(self):
        """The upper limit, if any, on the number of rows returned."""
        return self._limited_to

    @limited_to.setter
    def limited_to(self, limit):
        """The upper limit, if any, on the number of rows returned."""
        if isinstance(limit, int):
            limit = Limit(limit)
        else:
            assert isinstance(limit, Limit)
        self._limited_to = limit

    @property
    def order(self):
        """The ordering clause associated with this select statement."""
        return self._order

    @order.setter
    def order(self, order):
        """The ordering clause associated with this select statement."""
        if isinstance(order, OrderBy):
            self._order = order
        else:
            self._order = OrderBy(order)

    @property
    def grouping(self):
        """The grouping clause associated with this select statement."""
        return self._grouping

    @grouping.setter
    def grouping(self, grouping):
        """The grouping clause associated with this select statement."""
        if isinstance(grouping, GroupBy):
            self._grouping = grouping
        else:
            self._grouping = GroupBy(grouping)

    @property
    def width(self):
        """The number of fields selected in this select statement, or None if it is a select * statement."""
        if self._fields is None:
            return None
        return self._fields.width

    def from_(self, table):
        """Create a new select statement by adding a FROM clause."""
        return self._from(table)

    def distinct(self):
        """Create a new select statement by adding a distinct condition on the results."""
        result = self.copy()
        result.is_distinct = True
        return result

    def limit(self, limit):
        """Create a new select statement by applying an upper limit to the number of selected rows."""
        assert self._limited_to is None
        result = self.copy()
        result.limited_to = limit
        return result

    def order_by(self, *order):
        """Create a new select statement by applying an ordering to the results."""
        assert self._order is None

        if len(order) == 1 and isinstance(order[0], OrderBy):
            order = order[0]

        result = self.copy()
        result.order = order
        return result

    def group_by(self, *grouping):
        """Create a new select statement by applying a grouping to the results."""
        assert self._grouping is None

        if len(grouping) == 1 and isinstance(grouping[0], OrderBy):
            grouping = grouping[0]

        result = self.copy()
        result.grouping = grouping
        return result

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Select)
        return (
            same(self._distinct, other._distinct) and
            same(self._limited_to, other._limited_to) and
            same(self._order, other._order) and
            same(self._grouping, other._grouping)
        )


class Insert(SQLWriteCommand):
    """A SQL insert statement, as an AST."""

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
        """Create a new insert statement by adding an into clause."""
        return self._from(table)

    def where(self, clause):
        """Create a new insert statement by adding a where clause."""
        raise NotImplementedError("WHERE clauses are not supported for INSERT statements.")


class Update(SQLWriteCommand):
    """A SQL update statement, as an AST."""

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
        """Create a new update statement by adding a from clause."""
        return self._from(table)


class Delete(SQLCommand):
    """A SQL delete statement as an AST."""

    def __init__(self, table=None, where=None):
        super().__init__(table, fields=None, where=where)

    def _get_repr_args(self):
        return [
            # name, value, default (or NotImplemented), kw_required
            ('table', self._table, None, False),
            ('where', self._where_clause, None, False),
        ]

    def from_(self, table):
        """Create a new delete statement by adding a from clause."""
        return self._from(table)

    def fields(self, *fields):
        """UNSUPPORTED OPERATION"""
        raise NotImplementedError("Field lists are not supported for DELETE statements.")


class Identifier(SQLExpression):
    """A SQL identifier, such as a table or field name."""

    def __init__(self, path):
        path = tuple(path)
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
        """The "path" of the SQL identifier, meaning a tuple of the individual elements of the identifier."""
        return self._path

    @property
    def name(self):
        """The last element in the identifier's "path"."""
        return self._path[-1]

    @property
    def parent(self):
        """The parent identifier, meaning everything up to but not including the last element (the name) of this one."""
        if len(self._path) > 1:
            return Identifier(self._path[:-1])
        else:
            return None

    def get(self, name):
        """Create a child identifier by appending a new name to this identifier's path."""
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
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Identifier)
        return self._path == other._path


class TableExpression(SQLExpression, metaclass=ABCMeta):
    """
    Base class for table expressions.
    """

    @property
    @abstractmethod
    def leftmost(self):
        """Return the leftmost table in a table or join expression."""
        raise NotImplementedError()

    def join(self, table, on=None):
        """Create a new join statement by adding another table joined to this table or join."""
        return Join(JoinTypes.INNER, self, table, on)

    def inner_join(self, table, on=None):
        """Create a new inner join statement by adding another table inner-joined to this table or join."""
        return Join(JoinTypes.INNER, self, table, on)

    def left_join(self, table, on=None):
        """Create a new left join statement by adding another table left-joined to this table or join."""
        return Join(JoinTypes.LEFT, self, table, on)

    def right_join(self, table, on=None):
        """Create a new right join statement by adding another table right-joined to this table or join."""
        return Join(JoinTypes.RIGHT, self, table, on)

    def outer_join(self, table, on=None):
        """Create a new outer join statement by adding another table outer-joined to this table or join."""
        return Join(JoinTypes.OUTER, self, table, on)

    def full_join(self, table, on=None):
        """Create a new full join statement by adding another table full-joined to this table or join."""
        return Join(JoinTypes.OUTER, self, table, on)

    def select(self, *fields):
        """Create a new select statement from this table or join, optionally adding a field list."""
        if fields:
            return Select(self).fields(*fields)
        else:
            return Select(self)

    def update(self, *fields):
        """Create a new update statement from this table or join, optionally adding a field list."""
        if fields:
            return Update(self).fields(*fields)
        else:
            return Update(self)

    def insert(self, *fields):
        """Create a new insert statement from this table or join, optionally adding a field list."""
        if fields:
            return Insert(self).fields(*fields)
        else:
            return Insert(self)

    def delete(self):
        """Create a new delete statement from this table or join."""
        return Delete(self)

    @abstractmethod
    def get_field(self, field):
        """Get a field associated with this table or join."""
        raise NotImplementedError()


class Table(TableExpression):
    """A reference to a table within a SQL statement."""

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
        """The identifier representing this table."""
        return self._identifier

    @property
    def leftmost(self):
        """Return the leftmost table in a table or join expression."""
        return self

    def get_field(self, field):
        """Get a field associated with this table."""
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
        """Get the field corresponding to this identifier."""
        return Field(self._identifier)

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Table)
        return same(self._identifier, other._identifier)


class Join(TableExpression):
    """A SQL join statement."""

    def __init__(self, join_type, left, right, on=None):
        assert JoinTypes.is_valid(join_type)
        assert isinstance(left, TableExpression)
        assert isinstance(right, Table), "Right argument must be table. JOINS are left-associative."
        assert on is None or isinstance(on, On)

        # Make sure neither side is just the bare T object, but is actually a table or join.
        if isinstance(left, Table):
            assert left.identifier
        assert right.identifier

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
        """The type of join for this join statement. Must be a valid join type enumerated in JoinTypes."""
        return self._join_type

    @property
    def left(self):
        """The table or join appearing to the left of the join."""
        return self._left

    @property
    def right(self):
        """The table appearing to the right of the join."""
        return self._right

    @property
    def joined_tables(self):
        """An iterator over the joined tables, from left to right."""
        if isinstance(self._left, Table):
            yield self._left
        else:
            assert isinstance(self._left, Join)
            yield from self._left.joined_tables
        yield self._right

    @property
    def on_clause(self):
        """The ON clause associated with this join statement."""
        return self._on

    @on_clause.setter
    def on_clause(self, clause):
        """The ON clause associated with this join statement."""
        if isinstance(clause, Operation):
            clause = On(clause)
        else:
            assert isinstance(clause, On)
        self._on = clause

    def leftmost(self):
        """Return the leftmost table in a table or join expression."""
        current = self._left
        while isinstance(current._left, Join):
            current = current._left
            assert isinstance(current, (Join, Table))
        assert isinstance(current, Table)
        return current

    def on(self, clause):
        """Create a new join expression by adding an ON clause to this join."""
        assert self._on is None
        result = self.copy()
        result.on_clause = clause
        return result

    def using(self, field):
        """Create a new join expression by adding (the equivalent to) a USING clause to this join."""
        # TODO: Can we use a specialized construct here instead of doing this? Maybe a Using class,
        #       which can optionally be used in place of the On class for the on_clause, and which
        #       knows how to convert itself to an On clause for dialects that don't support USING?
        assert self._on is None
        if not isinstance(field, Field):
            field = Field(field)
        joined_tables = list(self.joined_tables)
        condition = None
        for index in range(len(joined_tables) - 1):
            left_field = joined_tables[index].get_field(field)
            right_field = joined_tables[index + 1].get_field(field)
            if condition is None:
                condition = (left_field == right_field)
            else:
                condition &= (left_field == right_field)
        result = self.copy()
        result.on_clause = On(condition)
        return result

    def get_field(self, field):
        """Get a field associated with this join."""
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
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Join)
        return (
            self._join_type == other._join_type and
            same(self._left, other._left) and
            same(other._right, self._right) and
            same(self._on, other._on)
        )


class Alias(SQLExpression):
    """An expression acting in the role of a selected field in a select statement."""

    def __init__(self, expression, alias=None):
        assert isinstance(expression, Value)

        if isinstance(alias, Field):
            assert alias.table is None
            alias = alias.identifier.name

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
        """The associated field name for this expression, if any."""
        return self._alias

    @property
    def expression(self):
        """The (possibly aliased) expression."""
        return self._expression

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Alias)
        return (
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
        """The individual entries in this field list."""
        return self._entries

    @property
    def width(self):
        """The number of entries in this field list."""
        return len(self._entries)

    def append(self, entry):
        """Add a new entry to the field list."""
        if isinstance(entry, Alias):
            alias = entry
        else:
            assert isinstance(entry, Field)
            alias = Alias(entry)

        self._entries += (alias,)

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, FieldList)
        return (
            self.width == other.width and
            all(same(mine, yours) for mine, yours in zip(self._entries, other._entries))
        )


class ValueList(SQLExpression):
    """
    A list of value_list in a SQL insert or update.
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
        """The individual entries in this value list."""
        return self._entries

    @property
    def width(self):
        """The number of entries in this value list."""
        return len(self._entries)

    def append(self, value):
        """Add a new entry to this value list."""
        assert isinstance(value, Value)
        self._entries += (value,)

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, ValueList)
        return (
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
        """The maximum number of rows."""
        return self._count

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Limit)
        return self._count == other._count


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
        """The condition(s) of this ON clause."""
        return self._condition

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, On)
        return same(self._condition, other._condition)


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
        """The condition(s) of this where clause."""
        return self._condition

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Where)
        return same(self._condition, other._condition)


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
        """The fields listed in this grouping clause."""
        return self._fields

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, GroupBy)
        return (
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
        """The field for this ordering clause entry."""
        return self._field

    @property
    def ascending(self):
        """Whether this field is ordered in ascending (vs. descending) order."""
        return self._ascending

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, OrderByEntry)
        return (
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
        """The entries in this ordering clause."""
        return self._entries

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, OrderBy)
        return (
            len(self._entries) == len(other._entries) and
            all(same(mine, yours) for mine, yours in zip(self._entries, other._entries))
        )


class Value(SQLExpression, metaclass=ABCMeta):
    """
    Base class for SQL expressions that represent values, e.g. field_list, parameters, literals,
    and the application of operators or functions to other value expressions.
    """

    def as_(self, alias):
        """Add an alias field name to this SQL value expression."""
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
        """Create a new SQL value expression representing the length of this string expression."""
        return Operation(Unary.LENGTH, self)

    def upper(self):
        """Create a new SQL value expression representing the upper case of this string expression."""
        return Operation(Unary.UPPER_CASE, self)

    def lower(self):
        """Create a new SQL value expression representing the lower case of this string expression."""
        return Operation(Unary.LOWER_CASE, self)

    def round(self, precision):
        """Create a new SQL value expression representing the rounding of this expression to a particular precision."""
        if not isinstance(precision, Value):
            precision = Literal(precision)
        return Operation(Binary.ROUND, self, precision)

    def format(self, form):
        """
        Create a new SQL value expression representing the formatting of this expression to a particular string format.
        """
        if not isinstance(form, Value):
            form = Literal(form)
        return Operation(Binary.FORMAT, self, form)

    def substring(self, start, length):
        """Create a new SQL value expression representing the substring of this string expression."""
        if not isinstance(start, Value):
            start = Literal(start)
        if not isinstance(length, Value):
            length = Literal(length)
        return Operation(Ternary.SUBSTRING, self, start, length)

    def if_else(self, condition, alternative):
        """Create a new if/else value expression which uses the given alternative when the condition is not met."""
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
        """The identifier representing this field."""
        return self._identifier

    @property
    def table(self):
        """The table to which this field belongs."""
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
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Field)
        return same(self._identifier, other._identifier)


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
        """The name of this parameter, if any."""
        return self._name

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
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
        """The corresponding Python value for this SQL literal value."""
        return self._value

    @property
    def sql_type(self):
        """The SQL value type for this SQL literal value."""
        return self._sql_type

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Literal)
        return (
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
        """The operator or function being applied in this SQL value expression."""
        return self._operator

    @property
    def operands(self):
        """The operands supplied to the operator or function in this SQL value expression."""
        return self._operands

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, Operation)
        return (
            self._operator == other._operator and
            len(self._operands) == len(other._operands) and
            all(same(mine, yours) for mine, yours in zip(self._operands, other._operands))
        )


class QueryValue(Value):
    """A SQL select statement, acting as a SQL value expression within another SQL statement."""

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
        """The SQL select statement acting as a value expression."""
        return self._query

    def same_as(self, other):
        """Check whether this SQL expression is the same as another. This method provides a way to check for equality
        between two ASTs. The normal means of doing so, the == operator, has been overridden to create equality
        comparison ASTs, rather than testing for equality between them."""
        if not super().same_as(other):
            return False
        assert isinstance(other, QueryValue)
        return same(self._query, other._query)


def select(*fields):
    """Create a new SQL select statement, optionally identifying which fields to select."""
    if fields:
        return Select().fields(*fields)
    else:
        return Select()


def update(*fields, **field_value_pairs):
    """
    Create a new SQL update statement, optionally identifying which fields to update or a set of field/value pairs.
    """
    assert not fields or not field_value_pairs
    if fields:
        return Update().fields(*fields)
    elif field_value_pairs:
        return Update().set(**field_value_pairs)
    else:
        return Update()


def insert(*fields, **field_value_pairs):
    """
    Create a new SQL insert statement, optionally identifying which fields to insert or a set of field/value pairs.
    """
    assert not fields or not field_value_pairs
    if fields:
        return Insert().fields(*fields)
    elif field_value_pairs:
        return Insert().set(**field_value_pairs)
    else:
        return Insert()


def delete():
    """Create a new SQL delete statement."""
    return Delete()
