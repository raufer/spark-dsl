import networkx as nx

from pydantic import BaseModel
from pydantic import validator

from garuda.constants.dimensions import DIMENSION

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


class Rule(BaseModel):
    """
    Currently, a Rule is the logical result of the combination of
    multiple boolean operations

    R :: (g) -> bool

    where `g` is a binary computational tree
    describing the association rules of the unitary elements
    """

    id: str
    name: str
    graph: Dict
    dimension: str = DIMENSION.NOT_DEFINED
    description: str = None

    @validator('graph')
    def check_graph(cls, g):
        if not _validate_computational_graph(g):
            raise ValueError('Invalid Graph')
        return g

    def __str__(self):
        string = f"Rule(id={self.id}, name={self.name}, dimension={self.dimension}, description={self.description})"
        return string

    @staticmethod
    def from_data(data):
        id = data['id']
        name = data['name']
        dimension = data.get('dimension', DIMENSION.NOT_DEFINED)
        description = data.get('description')
        graph = data['graph']
        return Rule(id=id, name=name, graph=graph, dimension=dimension, description=description)

