import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.models.dq.operation import Operation
from src.constants.argument_types import ARGUMENT_TYPES as AT
from src.constants.operations_ids import OPERATION_ID as OID
from src.models.dq.argument import Argument

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestOperation(SparkTestCase):

    def test_parse(self):

        data = {

        }

        {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        operation = Operation.from_data(data)
        arguments = [Argument(AT.COLUMN, F.col('age'))]
        self.assertEqual(operation.id, OID.NOT_NULL)
        self.assertListEqual(operation.arguments, arguments)

        data = {
          "id": OID.IS_BETWEEN,
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
        operation = Operation.from_data(data)
        arguments = [Argument(AT.COLUMN, F.col('salary')), Argument(AT.INTEGER, 70000), Argument(AT.INTEGER, 100000)]
        self.assertEqual(operation.id, OID.IS_BETWEEN)
        self.assertListEqual(operation.arguments, arguments)

