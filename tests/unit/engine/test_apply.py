import pyspark.sql.types as T

from src.constants.operations_ids import OPERATION_ID as OID
from src.engine.apply import apply_rule
from src.models.dq.rule import Rule

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestEngineApply(SparkTestCase):

    def test_apply_rule(self):

        data = [
            ('Joe', 30),
            ('Sue', 15)
        ]
        df = spark.createDataFrame(data, ['name', 'age'])

        f = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        graph = {
            'nodes': [
                {'id': 0, 'type': 'leaf', 'data': f}
            ],
            'edges': []
        }

        data = {
            'id': 'ID01',
            'name': 'rule-01',
            'graph': graph
        }
        rule = Rule.from_data(data)

        result = apply_rule(df, rule)

        data = [
            ('Joe', 30, True),
            ('Sue', None, False)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('rule-01', T.BooleanType(), False)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_apply_rule_2(self):

        data = [
            ('Joe', 30),
            ('Sue', None),
            (None, 40),
        ]
        df = spark.createDataFrame(data, ['name', 'age'])

        f = {
            'id': OID.IS_IN,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'name'
                },
                {
                    'type': 'list[string]',
                    'value': ['Joe', 'Tim']
                }
            ]
        }
        graph = {
            'nodes': [
                {'id': 0, 'type': 'leaf', 'data': f}
            ],
            'edges': []
        }

        data = {
            'id': 'ID01',
            'name': 'rule-01',
            'graph': graph
        }
        rule = Rule.from_data(data)

        result = apply_rule(df, rule)

        data = [
            ('Joe', 30, True),
            ('Sue', None, False),
            (None, 40, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('rule-01', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

