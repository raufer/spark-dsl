import operator
import logging

from typing import TypeVar
from typing import Generic
from typing import Callable

from garuda.engine.graph.constants import BRANCH_FUNCTIONS
from garuda.models.engine.column import Column

logger = logging.getLogger(__name__)


T = TypeVar('T')


class BranchNode(Generic[T]):
    """
    A Branch Node represents a function that receives a pair
    and returns a parameterized type `T`
    """

    def __init__(self, f: Callable[[Column[bool], Column[bool]], Column[bool]]):
        self.f = f

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __call__(self, *args, **kwargs):
        return self.f(*args, **kwargs)

    @staticmethod
    def from_data(data):
        function_id = data['function']

        if function_id == BRANCH_FUNCTIONS.AND:
            function = operator.and_
        elif function_id == BRANCH_FUNCTIONS.OR:
            function = operator.or_
        elif function_id == BRANCH_FUNCTIONS.XOR:
            function = operator.xor
        else:
            raise NotImplementedError(f"Unknown Branch Function '{function_id}'")

        return BranchNode(function)



