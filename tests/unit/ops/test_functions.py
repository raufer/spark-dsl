import unittest

import garuda.ops.functions as F
import pyspark.sql.functions as G
import pyspark.sql.types as T

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestFunctions(SparkTestCase):

    def test_not_null(self):

        data = [
            ('Joe', 30),
            ('Sue', None)
        ]
        df = spark.createDataFrame(data, ['name', 'age'])

        op = F.not_null(G.col('age'))
        result = df.withColumn('res', op)

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

    def test_is_between(self):

        data = [
            ('Joe', 30),
            ('Ana', 50),
            ('Sue', None)
        ]
        df = spark.createDataFrame(data, ['name', 'age'])

        op = F.is_between(G.col('age'), 30, 35)
        result = df.withColumn('res', op)

        data = [
            ('Joe', 30, True),
            ('Ana', 50, False),
            ('Sue', None, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('res', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_sum_greater_than(self):

        data = [
            ('Joe', 30, 20),
            ('Ana', 50, 40),
            ('Sue', None, 200),
            ('Bob', 200, None),
            ('Roy', None, None)
        ]
        df = spark.createDataFrame(data, ['name', 'score_1', 'score_2'])

        op = F.sum_greater_than(G.col('score_1'), G.col('score_2'), 85)
        result = df.withColumn('res', op)

        data = [
            ('Joe', 30, 20, False),
            ('Ana', 50, 40, True),
            ('Sue', None, 200, None),
            ('Bob', 200, None, None),
            ('Roy', None, None, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('score_1', T.LongType(), True),
            T.StructField('score_2', T.LongType(), True),
            T.StructField('res', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_is_in(self):

        data = [
            ('Joe', 'A'),
            ('Sue', 'B'),
            ('Ken', 'C'),
            ('Tim', 'D'),
            ('Ana', None),
            (None, None)
        ]
        df = spark.createDataFrame(data, ['name', 'code'])

        op = F.is_in(G.col('code'), ['A', 'D'])
        result = df.withColumn('res', op)

        data = [
            ('Joe', 'A', True),
            ('Sue', 'B', False),
            ('Ken', 'C', False),
            ('Tim', 'D', True),
            ('Ana', None, None),
            (None, None, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('code', T.StringType(), True),
            T.StructField('res', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)
        self.assertDataFrameEqual(result, expected)



