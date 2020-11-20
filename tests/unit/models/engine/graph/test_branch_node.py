import unittest
import operator

import pyspark.sql.types as T
import pyspark.sql.functions as F
from src.models.engine.graph.branch_node import BranchNode

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestEngineGraphBranchNode(SparkTestCase):

    def test_parse(self):
        data = {
            'function': '&',
        }
        node = BranchNode.from_data(data)
        self.assertEqual(node.f, operator.and_)

    def test_call(self):
        data = {
            'function': '&',
        }
        node = BranchNode.from_data(data)
        self.assertEqual(node(True, True), True)
        self.assertEqual(node(False, True), False)

