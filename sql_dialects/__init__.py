"""
sql_dialects is a library for generating SQL commands and queries in a dialect-agnostic way.

Usage:
    >>> from sql_dialects import *
    >>> update().set(column1=F.column2).from_(T.MyTable).compile()  # Default dialect is MySQL
    'UPDATE [MyTable] SET [column1]=[column2];'
    >>> set_default_dialect('T-SQL')  # Set a new default dialect
    >>> get_default_dialect()
    'T-SQL'
    >>> update().set(column1=F.column2).from_(T.MyTable).compile()
    'UPDATE [MyTable] SET [column1]=[column2] FROM [MyTable]'
    >>> select(F.Table1.A, F.Table2.A.as_(F.B)).\
    ... from_(T.Table1).join(T.Table2).\
    ... on(F.Table1.ID == F.Table2["Table1 ID"]).\
    ... compile('MySQL')  # Override the default dialect
    'SELECT [Table1].[A], [Table2].[A] AS [B] FROM [Table1] INNER JOIN [Table2] \
    ON [Table1].[ID] = [Table2].[Table1 ID]'
    >>>
"""

import sql_dialects.ast
import sql_dialects.dialects

from sql_dialects.ast import select, update, insert, delete
from sql_dialects.dialects import get_available_dialects, get_default_dialect, set_default_dialect


__author__ = 'Aaron Hosford'
__author_email__ = 'aaron.hosford@ericsson.com'
__version__ = '0.1'
__packages__ = ['sql_dialects', 'sql_dialects.dialects']
__url__ = 'https://scmgr.eams.ericsson.net/PythonLibs/sql_dialects'
__license__ = 'TBD'  # This will need to be open sourced if its dependents are.
__description__ = 'Dialect-agnostic construction of SQL commands'

# Registration of built-in plugins. See http://stackoverflow.com/a/9615473/4683578 for
# an explanation of how plugins work in the general case. Other, separately installable
# packages can register their own SQL dialects as plugins using this file as an example.
# They will automatically be made available by name when using this library.
__entry_points__ = {
    'sql_dialects': [
        'T-SQL = sql_dialects.dialects._t_sql:T_SQL',
        'MySQL = sql_dialects.dialects._mysql:MY_SQL',
        'SQLite = sql_dialects.dialects._sqlite:SQLITE',
        # TODO: Add other dialects, including Postgre
    ]
}

__all__ = [
    'T',
    'F',
    'V',
    'P',
    'select',
    'update',
    'insert',
    'delete',
    'get_available_dialects',
    'get_default_dialect',
    'set_default_dialect',
]


T = sql_dialects.ast.Table(None)
F = sql_dialects.ast.Field(None)


# noinspection PyPep8Naming
def V(value, sql_type=None):
    """
    Create a SQL value from a Python value.
    """
    return sql_dialects.ast.Literal(value, sql_type)


# noinspection PyPep8Naming
def P(name=None):
    """
    Create a SQL parameter.
    """
    return sql_dialects.ast.Parameter(name)


sql_dialects.dialects.REGISTRY.load()  # Load all registered dialects
set_default_dialect('MySQL')  # This seems to be the most commonly used
