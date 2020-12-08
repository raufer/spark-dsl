import networkx as nx
import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.engine.graph.eval import resolve_graph
from src.engine.graph.parse import parse_rule_computational_graph

from src.constants.dimensions import DIMENSION
from src.constants.operations_ids import OPERATION_ID as OID
from src.engine.graph.constants import NODE_TYPE
from src.models.dq.rule import Rule
from pyspark.sql import Column

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestModelsDQRule(SparkTestCase):

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
        graph = {
          'nodes': [
              {'id': 0, 'type': NODE_TYPE.LEAF,  'data': f}
          ],
          'edges': []
        }

        data = {
            'id': 'ID01',
            'name': 'rule-A',
            'graph': graph
        }
        rule = Rule(**data)

        self.assertEqual(rule.id, 'ID01')
        self.assertEqual(rule.name, 'rule-A')
        self.assertEqual(rule.dimension, DIMENSION.NOT_DEFINED)
        self.assertTrue(isinstance(parse_rule_computational_graph(rule.graph), nx.DiGraph))
        self.assertTrue(isinstance(resolve_graph(parse_rule_computational_graph(rule.graph)), Column))

    def test_parse_dimension(self):

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
                {'id': 0, 'type': NODE_TYPE.LEAF,  'data': f}
            ],
            'edges': []
        }

        data = {
            'id': 'ID01',
            'name': 'rule-A',
            'graph': graph,
            'dimension': DIMENSION.ACCURACY
        }
        rule = Rule(**data)

        self.assertEqual(rule.id, 'ID01')
        self.assertEqual(rule.name, 'rule-A')
        self.assertEqual(rule.dimension, DIMENSION.ACCURACY)
        self.assertTrue(isinstance(parse_rule_computational_graph(rule.graph), nx.DiGraph))
        self.assertTrue(isinstance(resolve_graph(parse_rule_computational_graph(rule.graph)), Column))


