import networkx as nx

from src.constants.dimensions import DIMENSION
from src.engine.graph.eval import resolve
from src.engine.graph.parse import parse_rule_computational_graph
from src.models.dq.argument import Argument

from typing import List
from typing import Dict

Graph = nx.DiGraph


class EntityMySQL(object):
    """
    An entity physically represented by a MySQL backend
    """

    def __init__(self, name: str, database: str, table: str):
        self.name = name
        self.database = database
        self.table = table

    def __str__(self):
        string = f"Entity<MySQL>(name={self.name}, db={self.database}, table={str(self.table)})"
        return string

    @staticmethod
    def from_data(data):
        name = data['name']
        database = data['database']
        table = data['table']
        return EntityMySQL(name, database, table)

