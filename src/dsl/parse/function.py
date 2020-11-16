import pyspark

import pyspark.sql.functions as F

from functools import reduce

from pyspark.sql import Column

from typing import Dict
from typing import List
from typing import Any

from src.dsl.constants import Types
from src.dsl.mappings.operations import DSL_FUNCTIONS


def _parse_arguments(args: List[Dict]) -> List[Any]:
    """
    Parses arguments
    """
    def reducer(acc, x):
        if x['type'] == Types.COLUMN:
            acc.append(F.col(x['value']))
        else:
            acc.append(x['value'])
        return acc

    args = reduce(reducer, args, [])
    return args


def parse_function(function: Dict) -> Column:
    """
    Given a DSL representation of a function,
    parse if into a valid spark computation
    """
    function_id = function['id']
    arguments = function['arguments']

    f = DSL_FUNCTIONS[function_id]
    args = _parse_arguments(arguments)

    return f(*args)

