import logging

import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from datetime import datetime

from garuda.engine.graph.eval import resolve_graph
from garuda.engine.graph.parse import parse_rule_computational_graph
from garuda.models.dq.package import Package
from garuda.models.dq.rule import Rule
from functools import reduce

from garuda.constants.tables.dq_results import DQResultsTable as DQ_TBL
from garuda.ops.transpose import transpose_columns_to_rows

logger = logging.getLogger(__name__)


def apply_rule(df: DataFrame, rule: Rule) -> DataFrame:
    """
    Appends a new column wih the logical result of
    the application of `rule`

    * the new column is set via `rule.name`
    """
    logger.debug(f"Applying rule '{str(rule)}'")
    graph = parse_rule_computational_graph(rule.graph)
    op = resolve_graph(graph)
    df = df.withColumn(rule.id, op)
    return df


def apply_package(df: DataFrame, package: Package) -> DataFrame:
    """
    Given a DataFrame initialized on top of the right entity,
    applies every rule described in `package` to it;

    Adds the following columns:
        PACKAGE_ID: the ID of the package (constant value)
        RULE_ID: the ID of each rule
        RESULT: a boolean with the logcal result of the application of a rule
        EXECUTION_TS: A timestamp value to mark the execution point in time

    Note that the number of lines in the dataframe is increased.
    If the original data set has length of N;
    and the number of rules is R;
    then the resulting dataframe has a total of `N x R` rows
    """
    logger.debug(f"Applying Package '{str(package)}'")

    df = df.withColumn(DQ_TBL.PACKAGE_ID, F.lit(package.id))
    df = reduce(lambda acc, x: apply_rule(acc, x), package.rules, df)

    columns = [r.id for r in package.rules]

    df = transpose_columns_to_rows(
        df=df,
        columns=columns,
        key_column=DQ_TBL.RULE_ID,
        value_column=DQ_TBL.RESULT
    )

    df = df.withColumn(DQ_TBL.EXECUTION_TS, F.lit(datetime.now()))

    return df
