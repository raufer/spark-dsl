import networkx as nx

from src.engine.graph.constants import NODE_TYPE
from src.models.dq.argument import Argument

from typing import List
from typing import Dict

from src.models.dq.operation import Operation

Graph = nx.DiGraph


def parse_rule_computational_graph(data: Dict) -> Graph:
    """
    Given a flat graph representation `data`, parses it into
    an internal graph representation

    `data` should contain:
        * nodes: a list of all of the nodes with local unique ids
        * edges: holds a list of the connections between the nodes, i.e. the association rules

    We assume two different types of nodes:
        * Leaf Nodes
        * Branch Nodes

    A Leaf Node represents a Boolean Column (Operation in DQ)
    while a Branch Node represents a function acting on two
    other nodes

    Example:

        rule :: f & g
        ```
        {
          nodes: [
            {'id': 0, type: 'leaf', data: <f>},
            {'id': 1, type: 'leaf', data: <g>},
            {'id': 2, type: 'branch', data: {'function': '&'}}
          ]
          edges: [(2, 0), (2, 1)]
        }
        ```
    """
    graph = nx.DiGraph()

    for node in data['nodes']:

        type = node['type']
        id = node['id']

        if type == NODE_TYPE.BRANCH:
            branch = BranchNode.from_data(node['data'])
            graph.add_node(id, type=NODE_TYPE.BRANCH, value=branch)
        elif type == NODE_TYPE.LEAF:
            op = Operation.from_data(node['data'])
            graph.add_node(id, type=NODE_TYPE.LEAF, value=op)
        else:
            raise NotImplementedError(f"Unknown node type '{type}'")

    return graph
