import pyspark.sql.functions as F
import pyspark.sql.types as T

from datetime import datetime

from garuda.constants.argument_types import ARGUMENT_TYPES
from garuda.constants.dimensions import DIMENSION
from garuda.constants.entities import ENTITY_TYPE
from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.constants.tables.dq_results import DQResultsTable as DQ_TBL

from garuda.engine.graph.constants import NODE_TYPE
from garuda.models.dq.package import Package
from garuda.models.dq.rule import Rule

from garuda.engine.apply import apply_rule
from garuda.engine.apply import apply_package

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestEngineApply(SparkTestCase):

    def test_apply_rule(self):

        data = [
            ('Joe', 30),
            ('Sue', None)
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
                {'id': 0, 'type': NODE_TYPE.LEAF, 'data': f}
            ],
            'edges': []
        }

        data = {
            '_id': 'ID01',
            'name': 'rule-01',
            'graph': graph
        }
        rule = Rule(**data)

        result = apply_rule(df, rule)

        data = [
            ('Joe', 30, True),
            ('Sue', None, False)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('ID01', T.BooleanType(), False)
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
                    'type': ARGUMENT_TYPES.LIST_STRINGS,
                    'value': ['Joe', 'Tim']
                }
            ]
        }
        graph = {
            'nodes': [
                {'id': 0, 'type': NODE_TYPE.LEAF, 'data': f}
            ],
            'edges': []
        }

        data = {
            '_id': 'ID01',
            'name': 'rule-01',
            'graph': graph
        }
        rule = Rule(**data)

        result = apply_rule(df, rule)

        data = [
            ('Joe', 30, True),
            ('Sue', None, False),
            (None, 40, None)
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField('ID01', T.BooleanType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        self.assertDataFrameEqual(result, expected)

    def test_apply_package(self):

        data = [
            ('Joe', 30),
            ('Joe', None),
            ('Tim', 20),
            ('Sue', 40),
            ('Sue', None),
            (None, None)
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
        g = {
            'id': OID.IS_IN,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'name'
                },
                {
                    'type': ARGUMENT_TYPES.LIST_STRINGS,
                    'value': ['Joe', 'Tim']
                }
            ]
        }
        branch_node = {
            'function': '&'
        }

        graph = {
            'nodes': [
                {'id': 0, 'type': NODE_TYPE.LEAF,  'data': f}
            ],
            'edges': []
        }
        rule_a = {
            '_id': 'ID01',
            'name': 'rule-A',
            'graph': graph,
            'dimension': DIMENSION.COMPLETNESS
        }

        graph = {
            'nodes': [
                {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': branch_node},
                {'id': 1, 'type': NODE_TYPE.LEAF, 'data': f},
                {'id': 2, 'type': NODE_TYPE.LEAF, 'data': g}
            ],
            'edges': [(0, 1), (0, 2)]
        }
        rule_b = {
            '_id': 'ID02',
            'name': 'rule-B',
            'graph': graph,
            'dimension': DIMENSION.ACCURACY
        }

        rules = [
            rule_a,
            rule_b
        ]

        entity = {
            'type': ENTITY_TYPE.SQL,
            'name': 'customer',
            'database': 'db',
            'table': 'table'
        }
        data = {
            '_id': 'PID01',
            'name': 'Package 01',
            'description': "Assessing the quality of Bruno's salary as a function of sales",
            'entity': entity,
            'rules': rules
        }

        package = Package(**data)
        result = apply_package(df, package)

        data = [
            ('Joe', 30, 'PID01', 'ID01', True, datetime.now()),
            ('Joe', 30, 'PID01', 'ID02', True, datetime.now()),
            ('Joe', None, 'PID01', 'ID01', False, datetime.now()),
            ('Joe', None, 'PID01', 'ID02', False, datetime.now()),
            ('Tim', 20, 'PID01', 'ID01', True, datetime.now()),
            ('Tim', 20, 'PID01', 'ID02', True, datetime.now()),
            ('Sue', 40, 'PID01', 'ID01', True, datetime.now()),
            ('Sue', 40, 'PID01', 'ID02', False, datetime.now()),
            ('Sue', None, 'PID01', 'ID01', False, datetime.now()),
            ('Sue', None, 'PID01', 'ID02', False, datetime.now()),
            (None, None, 'PID01', 'ID01', False, datetime.now()),
            (None, None, 'PID01', 'ID02', False, datetime.now())
        ]
        schema = T.StructType([
            T.StructField('name', T.StringType(), True),
            T.StructField('age', T.LongType(), True),
            T.StructField(DQ_TBL.PACKAGE_ID, T.StringType(), False),
            T.StructField(DQ_TBL.RULE_ID, T.StringType(), False),
            T.StructField(DQ_TBL.RESULT, T.BooleanType(), True),
            T.StructField(DQ_TBL.EXECUTION_TS, T.TimestampType(), True)
        ])
        expected = spark.createDataFrame(data, schema)

        result = result.withColumn(DQ_TBL.EXECUTION_TS, F.to_date(F.col(DQ_TBL.EXECUTION_TS), "yyyy-MM-dd"))
        expected = expected.withColumn(DQ_TBL.EXECUTION_TS, F.to_date(F.col(DQ_TBL.EXECUTION_TS), "yyyy-MM-dd"))

        self.assertDataFrameEqual(result, expected)

