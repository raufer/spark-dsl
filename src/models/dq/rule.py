from src.models.dq.argument import Argument

from typing import List
from typing import Dict


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

    def __init__(self, id: str, graph: Dict):
        self.id = id
        self.name
        self.arguments = arguments

    @staticmethod
    def from_data(data):
        id = data['id']
        arguments = [Argument.from_data(a) for a in data['arguments']]
        return Function(id, arguments)

