__author__ = 'Aaron Hosford'

from sql_dialects.dialects._base import SQLDialect
from sql_dialects.enums import Nullary, Unary, Binary, Ternary, JoinTypes, LiteralTypes
from sql_dialects import ast as sql
from sql_dialects.dialects import REGISTRY


class TSQLDialect(SQLDialect):

    def __init__(self):
        super().__init__('T-SQL')

        self._join_map = {
            JoinTypes.INNER: 'INNER JOIN',
            JoinTypes.OUTER: 'OUTER JOIN',
            JoinTypes.LEFT: 'LEFT JOIN',
            JoinTypes.RIGHT: 'RIGHT JOIN',
        }

        self._operations = {
            Nullary.NOW: 'GETDATE()',
            Nullary.GUID: 'NEWID()',
            Nullary.VERSION: '@@VERSION',

            Unary.NEG: '-{p1}',
            Unary.LENGTH: 'LEN({p1})',
            Unary.NOT: 'NOT {p1}',
            Unary.LOWER_CASE: 'LCASE({p1})',
            Unary.UPPER_CASE: 'UCASE({p1})',

            Binary.EQ: '{p1} = {p2}',
            Binary.NE: '{p1} != {p2}',
            Binary.LT: '{p1} < {p2}',
            Binary.GT: '{p1} > {p2}',
            Binary.LE: '{p1} <= {p2}',
            Binary.GE: '{p1} >= {p2}',
            Binary.AND: '{p1} AND {p2}',
            Binary.OR: '{p1} OR {p2}',
            Binary.ROUND: 'ROUND({p1},{p2})',
            Binary.FORMAT: 'FORMAT({p1},{p2})',

            Ternary.IF_ELSE: 'IF {p1} {p2} ELSE {p3}',
            Ternary.SUBSTRING: 'MID({p1}, {p2}, {p3})',
        }

    def build_select(self, tree):
        assert isinstance(tree, sql.Select)

        template = 'SELECT {top}{distinct}{fields} FROM {table}{where}{group_by}{order_by}'
        return template.format(
            top='TOP %s ' % tree.limited_to.count if tree.limited_to else '',
            distinct='DISTINCT ' if tree.is_distinct else '',
            fields=self.build_fields(tree.field_list, allow_aliases=True),
            table=self.build_table(tree.table, allow_joins=True),
            where=(' ' + self.build_where(tree.where_clause)) if tree.where_clause else '',
            group_by=(' ' + self.build_group_by(tree.grouping)) if tree.grouping else '',
            order_by=(' ' + self.build_order_by(tree.grouping)) if tree.grouping else ''
        )

    def build_insert(self, tree):
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
        assert isinstance(tree, sql.Update)

        template = 'UPDATE {table} SET {assignments} FROM {tables}{where}'
        return template.format(
            table=self.build_table(tree.table.leftmost, allow_joins=False),
            assignments=self.build_assignments(tree.field_list, tree.value_list),
            tables=self.build_table(tree.table, allow_joins=True),
            where=(' ' + self.build_where(tree.where_clause) if tree.where_clause else '')
        )

    def build_delete(self, tree):
        assert isinstance(tree, sql.Delete)

        template = 'DELETE FROM {table}{where}'
        return template.format(
            table=self.build_table(tree.table, allow_joins=False),
            where=(' ' + self.build_where(tree.where_clause) if tree.where_clause else '')
        )

    def build_fields(self, tree, *, allow_aliases):
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
        assert isinstance(tree, sql.TableExpression)
        assert isinstance(allow_joins, bool)

        if isinstance(tree, sql.Table):
            return '.'.join('[%s]' % element for element in tree.identifier.path)

        assert isinstance(tree, sql.Join)

        template = '{left} {join} {right}{on}'

        return template.format(
            left=self.build_table(tree.left, allow_joins=True),
            join=self._join_map[tree.join_type],
            right=self.build_table(tree.right, allow_joins=False),
            on=(' ' + self.build_on(tree.on)) if tree.on else ''
        )

    def build_where(self, tree):
        assert isinstance(tree, sql.Where)
        return 'WHERE %s' % self.build_value(tree.condition)

    def build_on(self, tree):
        assert isinstance(tree, sql.On)
        return 'ON %s' % self.build_value(tree.condition)

    def build_group_by(self, tree):
        assert isinstance(tree, sql.GroupBy)
        return 'GROUP BY ' + self.build_fields(tree.fields, allow_aliases=False)

    def build_order_by(self, tree):
        assert isinstance(tree, sql.OrderBy)
        return 'ORDER BY ' + ', '.join(self.build_order_by_entry(entry) for entry in tree.entries)

    def build_order_by_entry(self, tree):
        assert isinstance(tree, sql.OrderByEntry)
        return self.build_field(tree.field) + (' ASC' if tree.ascending else ' DESC')

    def build_field(self, tree, *, allow_aliases=False):
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
        if isinstance(tree, sql.Select):
            return '(%s)' % self.build_select(tree)
        else:
            assert isinstance(tree, sql.ValueList)
            return '(%s)' % ', '.join(self.build_value(entry) for entry in tree.entries)

    def build_assignments(self, fields, values):
        assert isinstance(fields, sql.FieldList)
        assert isinstance(values, sql.ValueList)
        assert fields.width == values.width
        return ', '.join('%s=%s' % (self.build_field(field, allow_aliases=True),
                                    self.build_value(value))
                         for field, value in zip(fields.entries, values.entries))

    def build_value(self, tree):
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
        assert isinstance(tree, sql.Literal)

        if tree.sql_type == LiteralTypes.STRING:
            return "'%s'" % tree.value.replace("'", "''")
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
        assert isinstance(tree, sql.Operation)

        params = {}
        for index, operand in enumerate(tree.operands):
            value = self.build_value(operand)
            if isinstance(operand, sql.Operation):
                value = '(%s)' % value
            params['p%s' % (index + 1)] = value

        return self._operations[tree.operator].format_map(params)


T_SQL = TSQLDialect()
REGISTRY.add(T_SQL)
