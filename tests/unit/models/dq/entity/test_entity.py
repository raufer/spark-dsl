import networkx as nx
import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from garuda.models.dq.entity.factory import make_entity
from garuda.models.dq.entity.sql import EntitySQL

from garuda.constants.entities import ENTITY_TYPE

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestModelsDQEntity(SparkTestCase):

    def test_parse(self):
        data = {
            'type': ENTITY_TYPE.SQL,
            'name': 'entity-A',
            'database': 'db',
            'table': 'table'
        }
        entity = EntitySQL(**data)

        self.assertTrue(isinstance(entity, EntitySQL))
        self.assertEqual(entity.name, 'entity-A')
        self.assertEqual(entity.database, 'db')
        self.assertEqual(entity.table, 'table')


