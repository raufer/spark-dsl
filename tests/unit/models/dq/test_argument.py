import unittest

from src.constants.argument_types import ARGUMENT_TYPES as T
from src.models.dq.argument import Argument


class TestArgument(unittest.TestCase):

    def test_parse(self):

        data = {'type': T.STRING, 'value': 'car'}
        argument = Argument.from_data(data)
        self.assertEqual(argument.type, T.STRING)
        self.assertEqual(argument.value, 'car')

        data = {'type': T.INTEGER, 'value': 10}
        argument = Argument.from_data(data)
        self.assertEqual(argument.type, T.INTEGER)
        self.assertEqual(argument.value, 10)

        data = {'type': T.BOOL, 'value': False}
        argument = Argument.from_data(data)
        self.assertEqual(argument.type, T.BOOL)
        self.assertEqual(argument.value, False)

        data = {'type': T.COLUMN, 'value': 'address'}
        argument = Argument.from_data(data)
        self.assertEqual(argument.type, T.COLUMN)
        self.assertEqual(argument.value, 'address')
