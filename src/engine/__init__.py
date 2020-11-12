import pyspark

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import DataFrame

from typing import Union
from typing import List
from typing import Dict

from src.dsl.parse.function import parse_function
from functools import reduce


def apply_functions(df: DataFrame, rules: Union[List[Dict], Dict]) -> DataFrame:
    """
    Applies a list of functions to a given dataframe

    * Returns the same number of rows
    * Each rule is materialized as a a column that is added to the dataframe
    * Each new column is of boolean value

    # TODO: how to handle names?
    """
    if isinstance(rules, dict):
        rules = [rules]

    names = [f'rule_{i}' for i in range(1, len(rules) + 1)]

    def reducer(acc, x):
        name, rule = x
        op = parse_function(rule)
        acc = acc.withColumn(name, op)
        return acc

    df = reduce(reducer, zip(names, rules), df)
    return df


