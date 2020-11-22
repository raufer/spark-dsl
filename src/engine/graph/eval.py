import networkx as nx

import pyspark.sql.functions as F

from functools import reduce

from src.engine.graph.constants import NODE_TYPE
from src.models.engine.column import Column

Graph = nx.DiGraph


def resolve(graph: Graph) -> Column[bool]:
    """
    Resolves a computational Graph and results a
    PySpark column that represents the computation
    """
    root = list(graph.nodes)[0]

    def _resolve(n):

        data = graph.nodes[n]

        if data['type'] == NODE_TYPE.BRANCH:
            function = data['value'].f
            args = [b for _, b in graph.out_edges(n)]
            args = [_resolve(arg) for arg in args]
            return function(*args)
        elif data['type'] == NODE_TYPE.LEAF:
            operation = data['value']
            return operation.op

    result = _resolve(root)
    return result