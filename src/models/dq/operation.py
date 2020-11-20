import logging

from src.dsl.mappings.operations import DSL_OPERATIONS
from src.models.dq.argument import Argument

from typing import Tuple

logger = logging.getLogger(__name__)


class Operation(object):
    """
    An operation represents a a Boolean Column as the result of
    a function application

    They are the most granular unit of computation of the engine

    Op :: (...) -> bool

    * each function is applicable to one or more columns;
    * the argument list can also contain other native types
    """

    def __init__(self, id: str, arguments: Tuple[Argument]):
        self.id = id
        self.arguments = arguments
        self.op = DSL_OPERATIONS[id](*[a.value for a in arguments])

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return all([
                self.id == other.id,
                self.arguments == other.arguments,
                str(self.op) == str(other.op)
            ])
        else:
            return False

    @staticmethod
    def from_data(data):
        id = data['id']
        arguments = tuple([Argument.from_data(a) for a in data['arguments']])
        return Operation(id, arguments)

