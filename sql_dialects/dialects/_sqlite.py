"""
Implementation of the SQLite SQL dialect.
"""


from ._base import SQLDialect

from sql_dialects.enums import Nullary, Unary, Binary, Ternary, JoinTypes, LiteralTypes, Aggregate
from sql_dialects.exceptions import OperationNotSupported
from sql_dialects import ast as sql
from sql_dialects.dialects import REGISTRY

__author__ = 'Aaron Hosford'


# Reference: http://www.tutorialspoint.com/sqlite/sqlite_using_joins.htm

class SQLiteDialect(SQLDialect):
    """The SQLite SQL dialect."""

    def __init__(self):
        super().__init__('SQLite')

        self._join_map = {
            JoinTypes.INNER: 'INNER JOIN',
            JoinTypes.OUTER: 'OUTER JOIN',
            JoinTypes.LEFT: 'LEFT JOIN',
            # JoinTypes.RIGHT: 'RIGHT JOIN',  # Not supported in SQLite
        }

        self._operations = {
            Nullary.NOW: "datetime('now')",
            # Nullary.GUID: 'NEWID()',  # Not supported in SQLite
            Nullary.VERSION: 'sqlite_version()',
            Nullary.RANDOM: 'RANDOM()',
            Nullary.CHANGES: 'CHANGES()',

            Unary.NEG: '-{p1}',
            Unary.LENGTH: 'LENGTH({p1})',
            Unary.NOT: 'NOT {p1}',
            Unary.LOWER_CASE: 'LOWER({p1})',
            Unary.UPPER_CASE: 'UPPER({p1})',
            Unary.ABS: 'ABS({p1})',

            Binary.EQ: '{p1} = {p2}',
            Binary.NE: '{p1} != {p2}',
            Binary.LT: '{p1} < {p2}',
            Binary.GT: '{p1} > {p2}',
            Binary.LE: '{p1} <= {p2}',
            Binary.GE: '{p1} >= {p2}',
            Binary.AND: '{p1} AND {p2}',
            Binary.OR: '{p1} OR {p2}',
            Binary.ROUND: 'ROUND({p1},{p2})',
            # Binary.FORMAT: 'FORMAT({p1},{p2})',  # Not supported in SQLite
            Binary.IF_NULL: 'IFNULL({p1},{p2})',

            Ternary.IF_ELSE: 'IF {p1} {p2} ELSE {p3}',
            Ternary.SUBSTRING: 'SUBSTR({p1}, {p2}, {p3})',

            Aggregate.AVG: 'AVG({p1})',
            Aggregate.COUNT: 'COUNT({p1})',
            Aggregate.MAX: 'MAX({p1})',
            Aggregate.MIN: 'MIN({p1})',
            Aggregate.SUM: 'SUM({p1})',
        }

    def build_select(self, tree):
        """Build a select statement from the given AST."""
        assert isinstance(tree, sql.Select)

        template = 'SELECT {distinct}{fields} FROM {table}{where}{group_by}{order_by}{limit}'
        return template.format(
            distinct='DISTINCT ' if tree.is_distinct else '',
            fields=self.build_fields(tree.field_list, allow_aliases=True),
            table=self.build_table(tree.table, allow_joins=True),
            where=(' ' + self.build_where(tree.where_clause)) if tree.where_clause else '',
            group_by=(' ' + self.build_group_by(tree.grouping)) if tree.grouping else '',
            order_by=(' ' + self.build_order_by(tree.grouping)) if tree.grouping else '',
            limit=' LIMIT %s' % tree.limited_to.count if tree.limited_to else ''
        )

    def build_insert(self, tree):
        """Build an insert statement from the given AST."""
        assert isinstance(tree, sql.Insert)

        template = 'INSERT INTO {table}{fields} VALUES {values}'
        return template.format(
            table=self.build_table(tree.table, allow_joins=False),
            fields=(
                (' (' + self.build_fields(tree.field_list, allow_aliases=False) + ')')
                if tree.field_list
                else ''
            ),
            values=self.build_values(tree.value_list)
        )

    def build_update(self, tree):
        """Build an update statement from the given AST."""
        assert isinstance(tree, sql.Update)

        template = 'UPDATE {tables} SET {assignments}{where}'
        return template.format(
            tables=self.build_table(tree.table, allow_joins=True),
            assignments=self.build_assignments(tree.field_list, tree.value_list),
            where=(' ' + self.build_where(tree.where_clause) if tree.where_clause else '')
        )

    def build_delete(self, tree):
        """Build a delete statement from the given AST."""
        assert isinstance(tree, sql.Delete)

        template = 'DELETE FROM {table}{where}'
        return template.format(
            table=self.build_table(tree.table, allow_joins=False),
            where=(' ' + self.build_where(tree.where_clause) if tree.where_clause else '')
        )

    def build_fields(self, tree, *, allow_aliases):
        """Build a field list from an AST."""
        assert tree is None or isinstance(tree, sql.FieldList)
        assert isinstance(allow_aliases, bool)

        if tree is None:
            return '*'

        fields = []
        for field in tree.entries:
            assert isinstance(field, sql.Alias)
            entry = self.build_value(field.expression)
            if allow_aliases:
                if field.name:
                    entry += ' AS [%s]' % field.name
            else:
                assert isinstance(field.expression, sql.Field)
                assert field.name is None
            fields.append(entry)
        return ', '.join(fields)

    def build_table(self, tree, *, allow_joins):
        """Build a table name or joined list of table names from an AST."""
        assert isinstance(tree, sql.TableExpression)
        assert isinstance(allow_joins, bool)

        if isinstance(tree, sql.Table):
            return '.'.join('[%s]' % element for element in tree.identifier.path)

        assert isinstance(tree, sql.Join)

        if tree.join_type not in self._join_map:
            raise OperationNotSupported(tree.join_type)

        template = '{left} {join} {right}{on}'

        return template.format(
            left=self.build_table(tree.left, allow_joins=True),
            join=self._join_map[tree.join_type],
            right=self.build_table(tree.right, allow_joins=False),
            on=(' ' + self.build_on(tree.on_clause)) if tree.on_clause else ''
        )

    def build_where(self, tree):
        """Build a where-clause from an AST."""
        assert isinstance(tree, sql.Where)
        return 'WHERE %s' % self.build_value(tree.condition)

    def build_on(self, tree):
        """Build an ON statement (table1 JOIN table2 ON ...) from an AST."""
        assert isinstance(tree, sql.On)
        return 'ON %s' % self.build_value(tree.condition)

    def build_group_by(self, tree):
        """Build a GROUP BY statement from an AST."""
        assert isinstance(tree, sql.GroupBy)
        return 'GROUP BY ' + self.build_fields(tree.fields, allow_aliases=False)

    def build_order_by(self, tree):
        """Build an ORDER BY statement from an AST."""
        assert isinstance(tree, sql.OrderBy)
        return 'ORDER BY ' + ', '.join(self.build_order_by_entry(entry) for entry in tree.entries)

    def build_order_by_entry(self, tree):
        """Build an entry in an ORDER BY field list from an AST."""
        assert isinstance(tree, sql.OrderByEntry)
        return self.build_field(tree.field) + (' ASC' if tree.ascending else ' DESC')

    def build_field(self, tree, *, allow_aliases=False):
        """Build a field in a field list from an AST."""
        if allow_aliases and isinstance(tree, sql.Alias):
            entry = self.build_value(tree.expression)
            if tree.name:
                entry += ' AS [%s]' % tree.name
            return entry

        assert isinstance(tree, sql.Field)
        if tree.table:
            table = self.build_table(tree.table, allow_joins=False)
            return '%s.[%s]' % (table, tree.identifier.name)
        else:
            return '[%s]' % tree.identifier.name

    def build_values(self, tree):
        """Build a value list from an AST."""
        if isinstance(tree, sql.Select):
            return '(%s)' % self.build_select(tree)
        else:
            assert isinstance(tree, sql.ValueList)
            return '(%s)' % ', '.join(self.build_value(entry) for entry in tree.entries)

    def build_assignments(self, fields, values):
        """Build a field/value assignment list from an AST."""
        assert isinstance(fields, sql.FieldList)
        assert isinstance(values, sql.ValueList)
        assert fields.width == values.width
        return ', '.join('%s=%s' % (self.build_field(field, allow_aliases=True),
                                    self.build_value(value))
                         for field, value in zip(fields.entries, values.entries))

    def build_value(self, tree):
        """Build a value from an AST."""
        assert isinstance(tree, (sql.Value, sql.Select))

        if isinstance(tree, sql.Field):
            return self.build_field(tree)
        elif isinstance(tree, sql.Parameter):
            return '?'
        elif isinstance(tree, sql.Literal):
            return self.build_literal(tree)
        elif isinstance(tree, sql.Operation):
            return self.build_operation(tree)
        else:
            assert isinstance(tree, sql.Select)
            return '(%s)' % self.build_select(tree)

    @staticmethod
    def build_literal(tree):
        """Build a literal value from an AST."""
        assert isinstance(tree, sql.Literal)

        if tree.sql_type == LiteralTypes.STRING:
            return "'%s'" % tree.value.replace("'", "''")
        elif tree.sql_type == LiteralTypes.BOOLEAN:
            # T-SQL can't handle true Booleans; it uses a single-bit int instead.
            return str(int(tree.value))
        elif tree.sql_type in (LiteralTypes.INTEGER, LiteralTypes.FLOAT):
            return str(tree.value)
        elif tree.sql_type == LiteralTypes.DATE:
            return "'%s'" % tree.value.strftime('%Y%m%d')
        elif tree.sql_type == LiteralTypes.DATE_TIME:
            return "'%s'" % tree.value.strftime('%Y%m%d %I:%M:%S %p')
        else:
            assert tree.sql_type == LiteralTypes.NULL
            return 'NULL'

    def build_operation(self, tree):
        """Build an operation (e.g. addition, subtraction, or function call) from an AST."""
        assert isinstance(tree, sql.Operation)

        if tree.operator not in self._operations:
            raise OperationNotSupported(tree.operator)

        params = {}
        for index, operand in enumerate(tree.operands):
            value = self.build_value(operand)
            if isinstance(operand, sql.Operation):
                value = '(%s)' % value
            params['p%s' % (index + 1)] = value

        return self._operations[tree.operator].format_map(params)


# Create the dialect and register it.
SQLITE = SQLiteDialect()
REGISTRY.add(SQLITE)
