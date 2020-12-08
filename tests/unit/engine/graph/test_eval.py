import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.constants.operations_ids import OPERATION_ID as OID
from src.engine.graph.constants import NODE_TYPE
from src.engine.graph.eval import resolve_graph

from src.engine.graph.parse import parse_rule_computational_graph
from src.models.dq.argument import Argument
from src.models.dq.operation import Operation
from src.models.engine.graph.branch_node import BranchNode

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestEngineGraphEval(SparkTestCase):

    def test_resolve_1(self):
        """
        rule :: f
        """
        f = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        data = {
          'nodes': [
              {'id': 0, 'type': 'leaf',  'data': f}
          ],
          'edges': []
        }

        graph = parse_rule_computational_graph(data)
        op = resolve_graph(graph)

        data = [
            ('Joe', 30),
            ('Sue', None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        result = df.withColumn('result', op)

        data = [
            ('Joe', 30, True),
            ('Sue', None, False)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('result', T.BooleanType(), False),
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_parse_2(self):
        """
        rule :: f & g
        """
        branch_node = {
            'function': '&'
        }
        f = {
            "id": OID.IS_BETWEEN,
            "arguments": [
                {
                    "type": "column",
                    "value": "age"
                },
                {
                    "type": "integer",
                    "value": 20
                },
                {
                    "type": "integer",
                    "value": 30
                }
            ]
        }
        g = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'name'
                }
            ]
        }
        data = {
            'nodes': [
                {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': branch_node},
                {'id': 1, 'type': NODE_TYPE.LEAF, 'data': f},
                {'id': 2, 'type': NODE_TYPE.LEAF, 'data': g}
            ],
            'edges': [
                (0, 1), (0, 2)
            ]
        }

        graph = parse_rule_computational_graph(data)
        op = resolve_graph(graph)

        data = [
            ('Joe', 30),
            ('Sue', 18),
            (None, 22),
            ('Bob', 40),
            ('Ana', None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        result = df.withColumn('result', op)

        data = [
            ('Joe', 30, True),
            ('Sue', 18, False),
            (None, 22, False),
            ('Bob', 40, False),
            ('Ana', None, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('result', T.BooleanType(), True),
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_parse_3(self):
        """
        rule :: (f & g) | (h & k)
        """
        branch_node_and = {
            'function': '&'
        }
        branch_node_or = {
            'function': '|'
        }
        f = {
            "id": OID.IS_BETWEEN,
            "arguments": [
                {
                    "type": "column",
                    "value": "age"
                },
                {
                    "type": "integer",
                    "value": 20
                },
                {
                    "type": "integer",
                    "value": 30
                }
            ]
        }
        g = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'name'
                }
            ]
        }
        h = {
            "id": OID.IS_BETWEEN,
            "arguments": [
                {
                    "type": "column",
                    "value": "age"
                },
                {
                    "type": "integer",
                    "value": 40
                },
                {
                    "type": "integer",
                    "value": 50
                }
            ]
        }
        k = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'salary'
                }
            ]
        }
        data = {
            'nodes': [
                {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': branch_node_or},
                {'id': 1, 'type': NODE_TYPE.BRANCH,  'data': branch_node_and},
                {'id': 2, 'type': NODE_TYPE.BRANCH,  'data': branch_node_and},
                {'id': 3, 'type': NODE_TYPE.LEAF, 'data': f},
                {'id': 4, 'type': NODE_TYPE.LEAF, 'data': g},
                {'id': 5, 'type': NODE_TYPE.LEAF, 'data': h},
                {'id': 6, 'type': NODE_TYPE.LEAF, 'data': k}
            ],
            'edges': [
                (0, 1), (0, 2),
                (1, 3), (1, 4),
                (2, 5), (2, 6)
            ]
        }

        graph = parse_rule_computational_graph(data)
        op = resolve_graph(graph)

        data = [
            ('Joe', 22, 15000),
            ('Sue', 28, None),
            ('Ana', 35, 20000),
            ('Pyk', 45, 20000),
            ('Jul', 45, None),

            ('Joe', None, 15000),
            ('Sue', None, None),
            ('Ana', None, 20000),
            ('Pyk', None, 20000),
            ('Jul', None, None),

            (None, 22, 15000),
            (None, 28, None),
            (None, 35, 20000),
            (None, 45, 20000),
            (None, 45, None),
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('salary', T.LongType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        result = df.withColumn('result', op)

        data = [
            ('Joe', 22, 15000, True),
            ('Sue', 28, None, True),
            ('Ana', 35, 20000, False),
            ('Pyk', 45, 20000, True),
            ('Jul', 45, None, False),

            ('Joe', None, 15000, None),
            ('Sue', None, None, None),
            ('Ana', None, 20000, None),
            ('Pyk', None, 20000, None),
            ('Jul', None, None, None),

            (None, 22, 15000, False),
            (None, 28, None, False),
            (None, 35, 20000, False),
            (None, 45, 20000, True),
            (None, 45, None, False)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('salary', T.LongType(), True),
            T.StructField('result', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)







