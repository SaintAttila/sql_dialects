from sql_dialects.dialects._base import SQLDialect
from sql_dialects.dialects._registry import DialectRegistry

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
    return sorted(dialect.name for dialect in REGISTRY)


def get_dialect(dialect):
    return REGISTRY[dialect]


def get_default_dialect():
    dialect = REGISTRY.default
    if dialect is None:
        return None
    else:
        return dialect.name


def set_default_dialect(dialect):
    REGISTRY.default = dialect


def register_dialect(dialect):
    REGISTRY.add(dialect)


def unregister_dialect(dialect):
    REGISTRY.remove(dialect)


def dialect_is_registered(dialect):
    return dialect in REGISTRY
