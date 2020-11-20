import unittest

import pyspark.sql.types as T
import pyspark.sql.functions as F

from src.models.dq.operation import Operation
from src.constants.argument_types import ARGUMENT_TYPES as AT
from src.constants.operations_ids import OPERATION_ID as OID
from src.models.dq.argument import Argument

from tests.utils.spark_test_case import SparkTestCase
from tests import spark


class TestOperation(SparkTestCase):

    def test_parse(self):
        pass
