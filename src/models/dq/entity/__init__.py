from abc import ABC
from abc import abstractmethod

from pydantic import BaseModel

from typing import Union


class Entity(BaseModel, ABC):
    """
    An entity can be materialized in different physical implementations
    Entity abstracts from the different backends that can host an entity

    Should contain some of the connection details
    """
    pass




