import unittest

from src.models.function import Function
from src.constants.argument_types import ARGUMENT_TYPES as T
from src.constants.function_ids import FUNCTION_ID as FID
from src.models.argument import Argument


class TestFunction(unittest.TestCase):

    def test_parse(self):

        data = {
            'id': FID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        function = Function.from_data(data)
        arguments = [Argument(T.COLUMN, 'age')]
        self.assertEqual(function.id, FID.NOT_NULL)
        self.assertListEqual(function.arguments, arguments)

        data = {
          "id": FID.IS_BETWEEN,
          "arguments": [
            {
              "type": "column",
              "value": "salary"
            },
            {
              "type": "integer",
              "value": 70000
            },
            {
              "type": "integer",
              "value": 100000
            }
          ]
        }
        function = Function.from_data(data)
        arguments = [Argument(T.COLUMN, 'salary'), Argument(T.INTEGER, 70000), Argument(T.INTEGER, 100000)]
        self.assertEqual(function.id, FID.IS_BETWEEN)
        self.assertListEqual(function.arguments, arguments)

