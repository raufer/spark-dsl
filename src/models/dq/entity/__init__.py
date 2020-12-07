from abc import ABC
from abc import abstractmethod

from typing import Union

from src.models.dq.entity.mysql import EntityMySQL


"""
An entity can be materialized in different physical implementations
Entity abstracts from the different backends that can host an entity

Should contain some of the connection details
"""

Entity = Union[
    EntityMySQL
]



