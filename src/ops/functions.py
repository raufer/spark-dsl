import pyspark

import pyspark.sql.functions as F
from pyspark.sql import Column

Col = Column
Bool = Column


def not_null(col: Col) -> Bool:
    """
    Checks if a column is not null
    """
    return col.isNotNull()


def is_between(col: Col, a: float, b: float) -> Bool:
    """
    Checks if a column is within a numerical range
    TODO: note that a NULL will not return in False; the NULL is propagated
    """
    op = (col >= a) & (col <= b)
    return op


def sum_greater_than(a: Col, b: Col, val: float) -> Bool:
    """
    Checks if the sum of two columns is bigger than a value
    """
    op = (a + b) > val
    return op
