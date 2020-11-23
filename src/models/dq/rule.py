import networkx as nx

from src.engine.graph.eval import resolve
from src.engine.graph.parse import parse_rule_computational_graph
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
    Currently, a Rule is the logical result of the combination of
    multiple boolean operations

    R :: (g) -> bool

    where `g` is a binary computational tree
    describing the association rules of the unitary elements

    #TODO: Implement `resolve graph`
    """

    def __init__(self, id: str, name: str, graph: Graph):
        self.id = id
        self.name = name
        self.graph = graph
        self.op = resolve(graph)

    @staticmethod
    def from_data(data):
        id = data['id']
        name = data['name']
        graph = parse_rule_computational_graph(data['graph'])
        return Rule(id, name, graph)

