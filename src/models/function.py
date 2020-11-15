
from src.models.argument import Argument
from typing import List


class Function(object):

    def __init__(self, id: str, arguments: List[Argument]):
        self.id = id
        self.arguments = arguments

    @staticmethod
    def from_data(data):
        id = data['id']
        arguments = [Argument.from_data(a) for a in data['arguments']]
        return Function(id, arguments)
