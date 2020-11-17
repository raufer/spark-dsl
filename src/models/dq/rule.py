import networkx as nx

from src.models.dq.argument import Argument

from typing import List
from typing import Dict


Graph = nx.DiGraph


def _validate_computational_graph(graph: Dict) -> bool:
    """
    Ensures the computation is properly declared
    according to the DSL specification
    TODO: implement
    """
    return True


class Rule(object):
    """
    A Rule is a composition of one or more function applications;

    R :: (g) -> bool

    where `g` is a binary computational tree
    describing how to unitary elements should be composed
    """

    def __init__(self, id: str, name: str, graph: Graph):
        self.id = id
        self.name = name
        self.graph = graph
        self.op = resolve(graph) -> Column[Boolean]

    @staticmethod
    def from_data(data):
        id = data['id']
        name = data['name']
        graph = _parse_rule_computational_graph(data['graph'])
        return Rule(id, name, graph)

