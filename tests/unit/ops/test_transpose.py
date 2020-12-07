import unittest

import src.ops.functions as F
import pyspark.sql.functions as G
import pyspark.sql.types as T

from src.ops.transpose import transpose_columns_to_rows
from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestOpsTranspose(SparkTestCase):

    def test_transpose_columns_to_rows(self):

        data = [
            ('Joe', 30, True, False),
            ('Sue', 20, False, True)
        ]
        df = spark.createDataFrame(data, ['name', 'age', 'rule-A', 'rule-B'])

        result = transpose_columns_to_rows(
            df=df,
            columns=['rule-A', 'rule-B']
        )
        data = [
            ('Joe', 30, 'rule-A', True),
            ('Joe', 30, 'rule-B', False),
            ('Sue', 20, 'rule-A', False),
            ('Sue', 20, 'rule-B', True)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('key', T.StringType(), False),
            T.StructField('value', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)
        self.assertDataFrameEqual(result, expected)

        result = transpose_columns_to_rows(
            df=df,
            columns=['rule-A', 'rule-B'],
            key_column='_rule',
            value_column='_result'
        )
        data = [
            ('Joe', 30, 'rule-A', True),
            ('Joe', 30, 'rule-B', False),
            ('Sue', 20, 'rule-A', False),
            ('Sue', 20, 'rule-B', True)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('_rule', T.StringType(), False),
            T.StructField('_result', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)
        self.assertDataFrameEqual(result, expected)
