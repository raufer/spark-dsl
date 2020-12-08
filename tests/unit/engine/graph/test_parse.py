import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.engine.graph.constants import NODE_TYPE

from garuda.engine.graph.parse import parse_rule_computational_graph
from garuda.models.dq.argument import Argument
from garuda.models.dq.operation import Operation
from garuda.models.engine.graph.branch_node import BranchNode

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
            'value': Operation(**f)
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
            'value': Operation(**f)
        }
        self.assertDictEqual(graph.nodes[1], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation(**g)
        }
        self.assertDictEqual(graph.nodes[2], expected_data)

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
        h = {
            'id': OID.NOT_NULL,
            'arguments': [
                {
                    'type': 'column',
                    'value': 'age'
                }
            ]
        }
        k = {
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

        self.assertListEqual(graph.nodes(), [0, 1, 2, 3, 4, 5, 6])

        expected_data = {
            'type': NODE_TYPE.BRANCH,
            'value': BranchNode.from_data(branch_node_or)
        }
        self.assertDictEqual(graph.nodes[0], expected_data)

        expected_data = {
            'type': NODE_TYPE.BRANCH,
            'value': BranchNode.from_data(branch_node_and)
        }
        self.assertDictEqual(graph.nodes[1], expected_data)

        expected_data = {
            'type': NODE_TYPE.BRANCH,
            'value': BranchNode.from_data(branch_node_and)
        }
        self.assertDictEqual(graph.nodes[2], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(f)
        }
        self.assertDictEqual(graph.nodes[3], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(g)
        }
        self.assertDictEqual(graph.nodes[4], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(f)
        }
        self.assertDictEqual(graph.nodes[4], expected_data)

        expected_data = {
            'type': NODE_TYPE.LEAF,
            'value': Operation.from_data(g)
        }
        self.assertDictEqual(graph.nodes[5], expected_data)







