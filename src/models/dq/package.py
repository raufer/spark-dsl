import networkx as nx

from typing import List
from typing import Dict

from src.models.dq.entity import Entity
from src.models.dq.entity.factory import make_entity
from src.models.dq.rule import Rule

Graph = nx.DiGraph


class Package(object):
    """
    Collects a logical set of rules to be applied to a given entity.
    From the point of view of the user, the package is the most granular unit of execution.

    rules :: [Rule]
    entity :: Entity -> connection details (storage-type-specific)
    """

    def __init__(self, id: str, name: str, description: str, entity: Entity, rules: List[Rule]):
        self.id = id
        self.name = name
        self.description = description
        self.entity = entity
        self.rules = rules

    def __str__(self):
        string = f"Package(id={self.id}, name={self.name}, description={str(self.description)}, entity={self.entity})"
        return string

    @staticmethod
    def from_data(data):
        id = data['id']
        name = data['name']
        description = data.get('description')
        entity = make_entity(data['entity'])
        rules = [Rule.from_data(r) for r in data['rules']]
        return Package(id, name, description, entity, rules)

