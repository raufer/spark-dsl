from pyspark.sql import Column as PySparkColumn
from typing import TypeVar, Generic

T = TypeVar('T')


class Column(PySparkColumn, Generic[T]):
    pass
