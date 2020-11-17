import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.engine.graph.parse import parse_rule_computational_graph
from src.constants.operations_ids import OPERATION_ID as OID
from src.models.dq.argument import Argument

from tests.utils.spark_test_case import SparkTestCase
# from tests import spark


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

        for n in graph.nodes()

    def test_parse_2(self):
        """
        rule :: f & g
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
                {'id': 0, 'type': 'leaf',  'data': f}
            ],
            'edges': []
        }

        graph = parse_rule_computational_graph(data)

        self.assertListEqual(graph.nodes(), [0])
        self.assertListEqual(graph.edges(), [])

    def test_parse_3(self):
        """
        rule :: (f & g) | (h & k)
        """

    def test_parse_4(self):
        """
        rule :: f | (g & h)
        """






