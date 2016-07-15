__author__ = 'Aaron Hosford'


class SQLDialectsError(Exception):
    """
    Base class for all exceptions defined in this library.
    """


class NoDefaultDialect(SQLDialectsError, KeyError):
    """
    Indicates that the default dialect was requested when no default dialect has been set.
    """