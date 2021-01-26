# DSL PoC

[Documentation](https://garuda-dq.atlassian.net/wiki/spaces/CONCEPTS/pages/4947983/DSL)

### Development

Python 3.6

Dev environment setup with:

```
poetry install
```

To install private dependencies
```

pip install -e git://github.com/{ username }/{ reponame }.git@{ tag name }#egg={ desired egg name }

```

e.g.

```
pip install git+https://raufer@bitbucket.org/garuda-dq/garuda.git@v0.1#egg=garuda
```

Or
```bash
poetry add git+ssh://raufer@bitbucket.org/garuda-dq/garuda.git
```

### Documentation


#### Hello-World Example

Run a full `Package`

```python

from pyspark.sql import SparkSession

from garuda.constants.argument_types import ARGUMENT_TYPES
from garuda.constants.dimensions import DIMENSION
from garuda.constants.entities import ENTITY_TYPE
from garuda.constants.operations_ids import OPERATION_ID as OID

from garuda.engine.graph.constants import NODE_TYPE
from garuda.models.dq.package import Package

from garuda.engine.apply import apply_package

spark = SparkSession.builder.master("local").getOrCreate()

data = [
    ('Joe', 30),
    ('Joe', None),
    ('Tim', 20),
    ('Sue', 40),
    ('Sue', None),
    (None, None)
]
df = spark.createDataFrame(data, ['name', 'age'])

df.show()

f = {
    'id': OID.NOT_NULL,
    'arguments': [
        {
            'type': 'column',
            'value': 'age'
        }
    ]
}
g = {
    'id': OID.IS_IN,
    'arguments': [
        {
            'type': 'column',
            'value': 'name'
        },
        {
            'type': ARGUMENT_TYPES.LIST_STRINGS,
            'value': ['Joe', 'Tim']
        }
    ]
}
branch_node = {
    'function': '&'
}

graph = {
    'nodes': [
        {'id': 0, 'type': NODE_TYPE.LEAF, 'data': f}
    ],
    'edges': []
}
rule_a = {
    'id': 'ID01',
    'name': 'rule-A',
    'graph': graph,
    'dimension': DIMENSION.COMPLETNESS
}

graph = {
    'nodes': [
        {'id': 0, 'type': NODE_TYPE.BRANCH, 'data': branch_node},
        {'id': 1, 'type': NODE_TYPE.LEAF, 'data': f},
        {'id': 2, 'type': NODE_TYPE.LEAF, 'data': g}
    ],
    'edges': [(0, 1), (0, 2)]
}
rule_b = {
    'id': 'ID02',
    'name': 'rule-B',
    'graph': graph,
    'dimension': DIMENSION.ACCURACY
}

rules = [
    rule_a,
    rule_b
]

entity = {
    'type': ENTITY_TYPE.SQL,
    'name': 'customer',
    'database': 'db',
    'table': 'table'
}
data = {
    'id': 'PID01',
    'name': 'Package 01',
    'description': "Assessing the quality of Bruno's salary as a function of sales",
    'entity': entity,
    'rules': rules
}

package = Package(**data)
result = apply_package(df, package)

result.show()

```

Output

```
Original
+----+----+
|name| age|
+----+----+
| Joe|  30|
| Joe|null|
| Tim|  20|
| Sue|  40|
| Sue|null|
|null|null|
+----+----+

Result
+----+----+-----------+--------+-------+--------------------------+
|name|age |_PACKAGE_ID|_RULE_ID|_RESULT|_EXECUTION_TS             |
+----+----+-----------+--------+-------+--------------------------+
|Joe |30  |PID01      |ID01    |true   |2021-01-26 18:30:43.378039|
|Joe |30  |PID01      |ID02    |true   |2021-01-26 18:30:43.378044|
|Joe |null|PID01      |ID01    |false  |2021-01-26 18:30:43.378044|
|Joe |null|PID01      |ID02    |false  |2021-01-26 18:30:43.378045|
|Tim |20  |PID01      |ID01    |true   |2021-01-26 18:30:43.378045|
|Tim |20  |PID01      |ID02    |true   |2021-01-26 18:30:43.378046|
|Sue |40  |PID01      |ID01    |true   |2021-01-26 18:30:43.378047|
|Sue |40  |PID01      |ID02    |false  |2021-01-26 18:30:43.378047|
|Sue |null|PID01      |ID01    |false  |2021-01-26 18:30:43.378048|
|Sue |null|PID01      |ID02    |false  |2021-01-26 18:30:43.378048|
|null|null|PID01      |ID01    |false  |2021-01-26 18:30:43.378049|
|null|null|PID01      |ID02    |false  |2021-01-26 18:30:43.378049|
+----+----+-----------+--------+-------+--------------------------+
```

Define a rule that just checks if a column is null.

```python
from pyspark.sql import SparkSession

from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.engine.apply import apply_rule
from garuda.models.dq.rule import Rule


spark = SparkSession.builder.master("local").getOrCreate()


data = [
    ('Joe', 30),
    ('Sue', None)
]
df = spark.createDataFrame(data, ['name', 'age'])
print('> original')
df.show()

f = {
    'id': OID.NOT_NULL,
    'arguments': [
        {
            'type': 'column',
            'value': 'age'
        }
    ]
}
graph = {
    'nodes': [
        {'id': 0, 'type': 'leaf', 'data': f}
    ],
    'edges': []
}

data = {
    'id': 'ID01',
    'name': 'rule-01',
    'graph': graph
}
rule = Rule.from_data(data)

result = apply_rule(df, rule)
print('> result')
result.show()
```

Output:

```
> original
+----+----+
|name| age|
+----+----+
| Joe|  30|
| Sue|null|
+----+----+

> result
+----+----+-------+
|name| age|rule-01|
+----+----+-------+
| Joe|  30|   true|
| Sue|null|  false|
+----+----+-------+
```


#### Computational Graph

A `Rule` within our framework is can be represented by a computational graph. A Binary Tree is sufficient for our purpose.

**Note:** Once we allow the composition of operations with non-boolean operations we might need something more than a binary tree.

Therefore we can represent a rule with a very simple (flat) data model:

```
nodes: [operations|function]
edges: [(int, int, op)]
```

In `nodes` we store a list of all of the nodes.

A node can be either an `Operation` node as described previously, or it can be a `Function` node, acting on two other nodes.
A `Operation` node is a leaf node; A `Function` is a branch node

The model for any node:

```
{
  id: <str>,
  data: {branch|leaf}
}
```

The model explicitly assigns a unique ID for every node with a scope that is restricted to the rule since we do not need global uniqueness.

edges then hold a list of the connections between the nodes, i.e. the association rules

e.g.

```
rule :: f
```

```python
f = {
    'id': 'not_null',
    'arguments': [
        {
            'type': 'column',
            'value': 'age'
        }
    ]
}
data = {
  'nodes': [
      {'id': 0, 'type': 'leaf',  'data': f}
  ],
  'edges': []
}
```

```
rule :: f & g 
```

```python
```python
from garuda.engine.graph.constants import NODE_TYPE


branch_node = {
    'function': '&'
}
f = {
    "id": 'is_between',
    "arguments": [
        {
            "type": "column",
            "value": "age"
        },
        {
            "type": "integer",
            "value": 20
        },
        {
            "type": "integer",
            "value": 30
        }
    ]
}
g = {
    'id': 'not_null',
    'arguments': [
        {
            'type': 'column',
            'value': 'name'
        }
    ]
}
data = {
    'nodes': [
        {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': branch_node},
        {'id': 1, 'type': NODE_TYPE.LEAF, 'data': f},
        {'id': 2, 'type': NODE_TYPE.LEAF, 'data': g}
    ],
    'edges': [
        (0, 1), (0, 2)
    ]
}
```


```
rule :: (f & g) | (h & k)
```

```python
```python
from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.engine.graph.constants import NODE_TYPE


branch_node_and = {
    'function': '&'
}
branch_node_or = {
    'function': '|'
}
f = {
    "id": OID.IS_BETWEEN,
    "arguments": [
        {
            "type": "column",
            "value": "age"
        },
        {
            "type": "integer",
            "value": 20
        },
        {
            "type": "integer",
            "value": 30
        }
    ]
}
g = {
    'id': OID.NOT_NULL,
    'arguments': [
        {
            'type': 'column',
            'value': 'name'
        }
    ]
}
h = {
    "id": OID.IS_BETWEEN,
    "arguments": [
        {
            "type": "column",
            "value": "age"
        },
        {
            "type": "integer",
            "value": 40
        },
        {
            "type": "integer",
            "value": 50
        }
    ]
}
k = {
    'id': OID.NOT_NULL,
    'arguments': [
        {
            'type': 'column',
            'value': 'salary'
        }
    ]
}
data = {
    'nodes': [
        {'id': 0, 'type': NODE_TYPE.BRANCH,  'data': branch_node_or},
        {'id': 1, 'type': NODE_TYPE.BRANCH,  'data': branch_node_and},
        {'id': 2, 'type': NODE_TYPE.BRANCH,  'data': branch_node_and},
        {'id': 3, 'type': NODE_TYPE.LEAF, 'data': f},
        {'id': 4, 'type': NODE_TYPE.LEAF, 'data': g},
        {'id': 5, 'type': NODE_TYPE.LEAF, 'data': h},
        {'id': 6, 'type': NODE_TYPE.LEAF, 'data': k}
    ],
    'edges': [
        (0, 1), (0, 2),
        (1, 3), (1, 4),
        (2, 5), (2, 6)
    ]
}
```

#### Rule

The simplest rule consists of just a single operation
```
rule :: f
```

```python
```python
from garuda.constants.operations_ids import OPERATION_ID as OID
from garuda.models.dq.rule import Rule


f = {
    'id': OID.NOT_NULL,
    'arguments': [
        {
            'type': 'column',
            'value': 'age'
        }
    ]
}
graph = {
  'nodes': [
      {'id': 0, 'type': 'leaf',  'data': f}
  ],
  'edges': []
}

data = {
    'id': 'ID01',
    'name': 'rule-A',
    'graph': graph
}

rule = Rule.from_data(data)
```

