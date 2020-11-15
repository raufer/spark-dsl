from typing import Any
from src.constants.argument_types import ARGUMENT_TYPES as T


NATIVE_TYPES = {
    T.INTEGER,
    T.BOOL,
    T.STRING,
    T.FLOAT
}


def _parse_value(type: str, value: Any) -> Any:
    """
    Parses the value dependent on the the given `type`
    Native types are naturally handed by the json -> dict parser

    TODO: handle non-native values (e.g. tuples, recursive)
    """
    if type in NATIVE_TYPES:
        return value

    if type == T.COLUMN:
        return value


class Argument(object):

    def __init__(self, type: str, value: Any):
        self.type = type
        self.value = value

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    @staticmethod
    def from_data(data):
        type = data['type']
        value = _parse_value(type, data['value'])
        return Argument(type, value)

