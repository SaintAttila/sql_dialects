from abc import ABCMeta, abstractmethod

import sql_dialects


__author__ = 'Aaron Hosford'


class SQLDialect(metaclass=ABCMeta):

    def __init__(self, name):
        assert name and isinstance(name, str)
        self._name = name

        self._handlers = {
            sql_dialects.ast.Select: self.build_select,
            sql_dialects.ast.Insert: self.build_insert,
            sql_dialects.ast.Update: self.build_update,
            sql_dialects.ast.Delete: self.build_delete,
        }

    @property
    def name(self):
        return self._name

    def build_command(self, tree):
        """
        Generate a SQL command from an AST.

        :param tree: The abstract syntax tree.
        :return: A SQL expression.
        """
        assert isinstance(tree, sql_dialects.ast.SQLCommand)
        assert type(tree) in self._handlers
        return self._handlers[type(tree)](tree)

    @abstractmethod
    def build_select(self, tree):
        assert isinstance(tree, sql_dialects.ast.Select)
        raise NotImplementedError()

    @abstractmethod
    def build_insert(self, tree):
        assert isinstance(tree, sql_dialects.ast.Insert)
        raise NotImplementedError()

    @abstractmethod
    def build_update(self, tree):
        assert isinstance(tree, sql_dialects.ast.Update)
        raise NotImplementedError()

    @abstractmethod
    def build_delete(self, tree):
        assert isinstance(tree, sql_dialects.ast.Delete)
        raise NotImplementedError()
