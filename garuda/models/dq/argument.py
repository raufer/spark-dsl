import logging

import pyspark.sql.functions as F

from pydantic import BaseModel
from pydantic import validator

from typing import Any
from garuda.constants.argument_types import ARGUMENT_TYPES as T


logger = logging.getLogger(__name__)


NATIVE_TYPES = {
    T.INTEGER,
    T.BOOL,
    T.STRING,
    T.FLOAT,
    T.LIST_STRINGS,
    T.LIST_INTEGERS,
    T.LIST_FLOAT
}


def _parse_value(type: str, value: Any) -> Any:
    """
    Parses the value dependent on the the given `type`
    Native types are naturally handed by the json -> dict parser

    TODO: handle non-native values (e.g. tuples, recursive)
    """
    if type in NATIVE_TYPES:
        return value

    elif type == T.COLUMN:
        return F.col(value)

    else:
        raise NotImplementedError(f"Unknown argument type '{type}'")


class Argument(BaseModel):

    type: str
    value: Any

    @validator('value')
    def validate_value(cls, v, values, **kwargs):
        return _parse_value(values['type'], v)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            if (self.type == other.type) and (self.type == T.COLUMN):
                return str(self.value) == str(other.value)
            else:
                return self.__dict__ == other.__dict__
        else:
            return False

    @staticmethod
    def from_data(data):
        type = data['type']
        value = _parse_value(type, data['value'])
        return Argument(type, value)

