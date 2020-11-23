from pyspark.sql import DataFrame
from src.models.dq.rule import Rule


def apply_rule(df: DataFrame, rule: Rule) -> DataFrame:
    """
    Appends a new column wih the logical result of
    the application of `rule`

    * the new column is set via `rule.name`
    """
    df = df.withColumn(rule.name, rule.op)
    return df