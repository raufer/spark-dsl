import unittest
import pyspark.sql.functions as F

from src.constants.argument_types import ARGUMENT_TYPES as T
from src.models.dq.argument import Argument

from tests import spark


class TestArgument(unittest.TestCase):

    def test_parse(self):

        data = {'type': T.STRING, 'value': 'car'}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.STRING)
        self.assertEqual(argument.value, 'car')

        data = {'type': T.INTEGER, 'value': 10}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.INTEGER)
        self.assertEqual(argument.value, 10)

        data = {'type': T.BOOL, 'value': False}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.BOOL)
        self.assertEqual(argument.value, False)

        data = {'type': T.COLUMN, 'value': 'address'}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.COLUMN)
        self.assertEqual(str(argument.value), str(F.col('address')))

    def test_parse_list_types(self):

        data = {'type': T.LIST_FLOAT, 'value': [0.1, 0.2]}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.LIST_FLOAT)
        self.assertEqual(argument.value, [0.1, 0.2])

        data = {'type': T.LIST_INTEGERS, 'value': [1, 2, 3]}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.LIST_INTEGERS)
        self.assertEqual(argument.value, [1, 2, 3])

        data = {'type': T.LIST_STRINGS, 'value': ['A', 'B']}
        argument = Argument(**data)
        self.assertEqual(argument.type, T.LIST_STRINGS)
        self.assertEqual(argument.value, ['A', 'B'])

