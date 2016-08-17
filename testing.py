import datetime
import unittest

from sql_dialects import T, F, V, P


# noinspection PyPep8Naming,PyAttributeOutsideInit
class DialectTestCase:
    """Mixin class for universally applicable dialect-specific test cases."""

    DIALECT_NAME = None

    TREES = (
        T.table1.select(
            F.table1.field1,
            F.table1.field2
        ).from_(
            T.table1
        ).where(
            F.table1.field3 == 'abc'
        ),

        T.table1.update(
            field1=13,
            field2=datetime.datetime.now()
        ).where(
            row_id=55
        )
    )

    TARGETS = None

    def setUp(self):
        assert self.DIALECT_NAME is not None, "Dialect name must be provided for dialect test cases."
        assert self.TARGETS is not None, "Targets must be provided for dialect test cases."
        assert len(self.TARGETS) == len(self.TREES), "Number of targets does not match number of trees."

    def _test(self, index):
        tree = self.TREES[index]
        target = self.TARGETS[index]
        result = tree.compile(self.DIALECT_NAME)

        assert result == target, result

    def test0(self):
        self._test(0)


class MySQLTestCase(unittest.TestCase, DialectTestCase):

    DIALECT_NAME = 'MySQL'
    TARGETS = (
        "SELECT [table1].[field1], [table1].[field2] "
        "FROM [table1] "
        "WHERE [table1].[field3] = 'abc';",

        # TODO: Is this the correct date/time format?
        "UPDATE [table1] "
        "SET [table1].[field2]='20160817 03:54:02 PM', [table1].[field1]=13 "
        "WHERE [row_id] = 55;"
    )


class TSQLTestCase(unittest.TestCase, DialectTestCase):

    DIALECT_NAME = 'T-SQL'
    TARGETS = (
        "SELECT [table1].[field1], [table1].[field2] "
        "FROM [table1] "
        "WHERE [table1].[field3] = 'abc'",

        # TODO: Is this the correct date/time format?
        "UPDATE [table1] "
        "SET [table1].[field2]='20160817 03:53:29 PM', [table1].[field1]=13 "
        "FROM [table1] "
        "WHERE [row_id] = 55"
    )


if __name__ == "__main__":
    unittest.main()
