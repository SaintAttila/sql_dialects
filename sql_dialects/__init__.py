"""
all_sql is a library for generating SQL commands and queries in a dialect-agnostic way.
"""

import sql_dialects.ast
import sql_dialects.dialects

from sql_dialects.ast import select, update, insert, delete
from sql_dialects.dialects import get_available_dialects, get_default_dialect, set_default_dialect


__author__ = 'Aaron Hosford'
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


def V(value, sql_type=None):
    """
    Create a SQL value from a Python value.
    """
    return sql_dialects.ast.Literal(value, sql_type)


def P(name=None):
    """
    Create a SQL parameter.
    """
    return sql_dialects.ast.Parameter(name)


sql_dialects.dialects.REGISTRY.load()
sql_dialects.set_default_dialect('T-SQL')
