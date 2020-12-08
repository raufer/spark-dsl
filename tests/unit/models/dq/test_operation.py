import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.engine.graph.eval import resolve_operation
from src.models.dq.operation import Operation
from src.constants.argument_types import ARGUMENT_TYPES as AT
from src.constants.operations_ids import OPERATION_ID as OID
from src.models.dq.argument import Argument

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestOperation(SparkTestCase):

    def test_parse(self):

        data = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        operation = Operation(**data)
        arguments = [Argument(type=AT.COLUMN, value='age')]
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
        operation = Operation(**data)
        arguments = [
            Argument(type=AT.COLUMN, value='salary'),
            Argument(type=AT.INTEGER, value=70000),
            Argument(type=AT.INTEGER, value=100000)
        ]
        self.assertEqual(operation.id, OID.IS_BETWEEN)
        self.assertListEqual(operation.arguments, arguments)

    def test_call(self):

        data = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        operation = Operation(**data)

        data = [
            ('Joe', 30),
            ('Sue', None)
        ]
        df = spark.createDataFrame(data, ['name', 'age'])
        result = df.withColumn('res', resolve_operation(operation))

        data = [
            ('Joe', 30, True),
            ('Sue', None, False)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('res', T.BooleanType(), False)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)





