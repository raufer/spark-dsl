import logging

from pydantic import BaseModel
from pydantic import validator

from garuda.dsl.mappings.operations import DSL_OPERATIONS
from garuda.models.dq.argument import Argument

from typing import List

logger = logging.getLogger(__name__)


class Operation(BaseModel):
    """
    An operation represents a a Boolean Column as the result of
    a function application

    They are the most granular unit of computation of the engine

    Op :: (...) -> bool

    * each function is applicable to one or more columns;
    * the argument list can also contain other native types
    """
    id: str
    arguments: List[Argument]

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all([
                self.id == other.id,
                self.arguments == other.arguments,
            ])
        else:
            return False

    @staticmethod
    def from_data(data):
        id = data['id']
        arguments = data['arguments']
        return Operation(id=id, arguments=arguments)

