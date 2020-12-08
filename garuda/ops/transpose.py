import unittest

from pyspark.sql import DataFrame

import pyspark.sql.functions as F
import pyspark.sql.types as T

from typing import List


def transpose_columns_to_rows(df: DataFrame, columns: List[str], key_column: str = 'key', value_column: str = 'value') -> DataFrame:
    """
    Given a dataset transposes a subset of columns to rows, by exploding
    the number of rows (x |S|)

    >> input
    +----+---+------+------+
    |name|age|rule-A|rule-B|
    +----+---+------+------+
    | Joe| 30|  true| false|
    | Sue| 20| false|  true|
    +----+---+------+------+

    >> output
    +----+---+------+-----+
    |name|age|   key|value|
    +----+---+------+-----+
    | Joe| 30|rule-A| true|
    | Joe| 30|rule-A|false|
    | Sue| 20|rule-B|false|
    | Sue| 20|rule-B| true|
    +----+---+------+-----+
    """
    cols_to_explode, dtypes = zip(*((c, t) for (c, t) in df.dtypes if c in columns))
    by = [c for c, _ in df.dtypes if c not in columns]

    if not len(set(dtypes)) == 1:
        raise ValueError(f"All columns have to be of the same type. Spark SQL supports only homogeneous columns")

    # Create and explode an array of (column_name, column_value) structs
    kvs = F.explode(F.array([
        F.struct(F.lit(c).alias(key_column), F.col(c).alias(value_column)) for c in cols_to_explode
    ])).alias("kvs")

    return df.select(by + [kvs]).select(by + [f"kvs.{key_column}", f"kvs.{value_column}"])


