import networkx as nx
import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from garuda.constants.dimensions import DIMENSION
from garuda.constants.entities import ENTITY_TYPE
from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.engine.graph.constants import NODE_TYPE
from garuda.models.dq.entity.factory import make_entity
from garuda.models.dq.entity.sql import EntitySQL
from garuda.models.dq.package import Package
from garuda.models.dq.rule import Rule
from pyspark.sql import Column

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestModelsDQPackage(SparkTestCase):

    def test_parse(self):
        
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
                    'type': 'list[string]',
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
            'dividend': 1,
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

        self.assertEqual(package.id, 'PID01')
        self.assertEqual(package.name, 'Package 01')
        self.assertEqual(package.description, "Assessing the quality of Bruno's salary as a function of sales")

        rules = package.rules
        self.assertEqual(rules[0].id, 'ID01')
        self.assertEqual(rules[0].name, 'rule-A')
        self.assertEqual(rules[0].dimension, DIMENSION.COMPLETNESS)
        self.assertEqual(rules[1].id, 'ID02')
        self.assertEqual(rules[1].name, 'rule-B')
        self.assertEqual(rules[1].dimension, DIMENSION.ACCURACY)

        entity = package.entity
        self.assertTrue(isinstance(entity, EntitySQL))
        self.assertEqual(entity.name, 'customer')
        self.assertEqual(entity.database, 'db')
        self.assertEqual(entity.table, 'table')
