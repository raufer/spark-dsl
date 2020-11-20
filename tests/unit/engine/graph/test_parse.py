import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.constants.operations_ids import OPERATION_ID as OID
from src.engine.graph.constants import NODE_TYPE

from src.engine.graph.parse import parse_rule_computational_graph
from src.models.dq.argument import Argument
from src.models.dq.operation import Operation
from src.models.engine.graph.branch_node import BranchNode

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestGraphParse(SparkTestCase):

    def test_parse(self):
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

        self.assertListEqual(graph.nodes(), [0])
        self.assertListEqual(graph.edges(), [])

        expected_data_0 = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(f)
        }

        self.assertDictEqual(graph.nodes[0], expected_data_0)

    def test_parse_2(self):
        """
        rule :: f & g
        """
        branch_node = {
            'function': '&'
        }
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
                {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': {'function': '&'}},
                {'id': 1, 'type': NODE_TYPE.LEAF, 'data': f},
                {'id': 2, 'type': NODE_TYPE.LEAF, 'data': g}
            ],
            'edges': [
                (0, 1), (0, 2)
            ]
        }

        graph = parse_rule_computational_graph(data)

        self.assertListEqual(graph.nodes(), [0, 1, 2])
        self.assertListEqual(graph.edges(), [(0, 1), (0, 2)])

        expected_data = {
            'type': NODE_TYPE.BRANCH,
            'value': BranchNode.from_data(branch_node)
        }
        self.assertDictEqual(graph.nodes[0], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(f)
        }
        self.assertDictEqual(graph.nodes[1], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(g)
        }
        self.assertDictEqual(graph.nodes[2], expected_data)

    def test_parse_3(self):
        """
        rule :: (f & g) | (h & k)
        """

    def test_parse_4(self):
        """
        rule :: f | (g & h)
        """






