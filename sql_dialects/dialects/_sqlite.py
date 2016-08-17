"""
Implementation of the SQLite SQL dialect.
"""


from ._base import SQLDialect

__author__ = 'Aaron Hosford'


class SQLiteDialect(SQLDialect):
    """The SQLite SQL dialect."""

    def __init__(self):
        super().__init__('SQLite')
