import pyspark
import pyspark.sql.functions as F

from src.models.engine.column import Column
from typing import Any
from typing import List


def not_null(col: Column[Any]) -> Column[bool]:
    """
    Checks if a column is not null
    """
    return col.isNotNull()


def is_between(col: Column[Any], a: float, b: float) -> Column[bool]:
    """
    Checks if a column is within a numerical range
    TODO: note that a NULL will not return in False; the NULL is propagated
    """
    op = (col >= a) & (col <= b)
    return op


def sum_greater_than(a: Column[Any], b: Column[Any], val: float) -> Column[bool]:
    """
    Checks if the sum of two columns is bigger than a value
    """
    op = (a + b) > val
    return op


def is_in(col: Column[Any], values: List[Any]) -> Column[bool]:
    """
    Checks if column respects a given set of values
    """
    op = col.isin(values)
    return op
