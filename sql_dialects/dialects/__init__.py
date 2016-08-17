"""
Dialect management routines.
"""

from ._base import SQLDialect
from ._registry import DialectRegistry

__author__ = 'Aaron Hosford'
__all__ = [
    'SQLDialect',
    'get_available_dialects',
    'get_default_dialect',
    'set_default_dialect',
    'register_dialect',
    'unregister_dialect',
    'dialect_is_registered',
]


REGISTRY = DialectRegistry()


def get_available_dialects():
    """Return a sorted list of the names of all currently available SQL dialects."""
    return sorted(dialect.name for dialect in REGISTRY)


def get_dialect(dialect):
    """Return the SQLDialect instance associated with this SQL dialect name."""
    return REGISTRY[dialect]


def get_default_dialect():
    """Return the name of the default SQL dialect."""
    dialect = REGISTRY.default
    if dialect is None:
        return None
    else:
        return dialect.name


def set_default_dialect(dialect):
    """
    Set the default dialect to the indicated one. The dialect parameter may be a SQLDialect instance or the name of a
    registered dialect.
    """
    REGISTRY.default = dialect


def register_dialect(dialect):
    """Register a new SQL dialect. The dialect parameter must be a SQLDialect instance."""
    REGISTRY.add(dialect)


def unregister_dialect(dialect):
    """Unregister a SQL dialect. The dialect parameter may be a registered SQLDialect instance or the name of a
    registered dialect."""
    REGISTRY.remove(dialect)


def dialect_is_registered(dialect):
    """Return a bool indicating whether the given dialect is currently registered. The dialect parameter may be either
    a SQLDialect instance or the name of a dialect."""
    return dialect in REGISTRY
