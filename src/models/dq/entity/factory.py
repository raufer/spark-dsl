from src.constants.entities import ENTITY_TYPE
from src.models.dq.entity import Entity

from typing import Dict
from src.models.dq.entity.sql import EntitySQL


def make_entity(data: Dict) -> Entity:
    """
    Creates and returns an entity object

    An entity can be materialized in different physical implementations
    Entity abstracts from the different backends that can host an entity
    """

    type = data['type']

    if type == ENTITY_TYPE.MYSQL:
        return EntitySQL(**data)

    else:
        raise NotImplementedError(f"Unknown entity type '{type}'")
