import logging

from pyspark.sql import DataFrame
from src.models.dq.rule import Rule


logger = logging.getLogger(__name__)


def apply_rule(df: DataFrame, rule: Rule) -> DataFrame:
    """
    Appends a new column wih the logical result of
    the application of `rule`

    * the new column is set via `rule.name`
    """
    logger.debug(f"Applying rule '{str(rule)}'")
    df = df.withColumn(rule.name, rule.op)
    return df