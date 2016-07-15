__author__ = 'Aaron Hosford'


from sql_dialects.dialects._base import SQLDialect


class SQLiteDialect(SQLDialect):

    def __init__(self):
        super().__init__('SQLite')
