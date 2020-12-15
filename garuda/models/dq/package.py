import networkx as nx

from typing import List
from typing import Optional
from typing import Dict
from typing import Union
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import validator

from garuda.models.dq.entity.sql import EntitySQL

from garuda.models.dq.entity.factory import make_entity
from garuda.models.dq.rule import Rule

Graph = nx.DiGraph


class Package(BaseModel):
    """
    Collects a logical set of rules to be applied to a given entity.
    From the point of view of the user, the package is the most granular unit of execution.

    rules :: [Rule]
    entity :: Entity -> connection details (storage-type-specific)
    """
    id: Optional[Any] = Field(None, alias='_id')
    id_ref: Optional[Any] = Field(None, alias='_id_ref')

    revision: Optional[int]
    name: str
    entity: Union[EntitySQL]
    rules: List[Rule]
    description: str = None

    @validator('id', 'id_ref')
    def validate_id(cls, v):
        if not isinstance(v, str):
            v = str(v)
        return v

    def __str__(self):
        string = f"Package(id={self.id}, name={self.name}, description={str(self.description)}, entity={self.entity}, n_rules={len(self.rules)})"
        return string

    @staticmethod
    def from_data(data):
        id = data['id']
        name = data['name']
        description = data.get('description')
        entity = make_entity(data['entity'])
        rules = data['rules']
        return Package(id=id, name=name, description=description, entity=entity, rules=rules)


if __name__ == '__main__':

    class A:
        ...

    data = {
        '_id': A(),
        'name': 'V'
    }

    print(Package(**data))
