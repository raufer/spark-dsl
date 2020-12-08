import networkx as nx

from pydantic import BaseModel
from pydantic import validator

from garuda.constants.entities import ENTITY_TYPE
from garuda.models.dq.entity import Entity

Graph = nx.DiGraph


class EntitySQL(Entity):
    """
    An entity physically represented by a traditional SQL backend
    """
    name: str
    database: str
    table: str

    def __str__(self):
        string = f"Entity<SQL>(name={self.name}, db={self.database}, table={str(self.table)})"
        return string

