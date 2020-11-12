import pyspark

import pyspark.sql.functions as F
import pyspark.sql.types as T

from src import spark
from src.engine import apply_functions
from src.ops.io.files import read_json


data = [
    ('Joe', 42, 98000, 'City A'),
    ('Sue', 49, 83000, 'City B'),
    ('Bob', 30, 72000, 'City C'),
    ('Jon', 42, 99000, 'City D'),
    ('Kyu', 23, 92000, 'City E'),
    ('Raj', 41, 89000, 'City F'),
    ('Bruno', 29, 42000, 'Herdade Parra'),
    ('Roy', 29, 79000, None)
]

df = spark.createDataFrame(data, ['name', 'age', 'salary', 'address'])

df.show()

rules = [
    read_json('/Users/raulferreira/garuda/poc-dsl/rules/is_between.json'),
    read_json('/Users/raulferreira/garuda/poc-dsl/rules/is_null.json'),
    read_json('/Users/raulferreira/garuda/poc-dsl/rules/sum_greater_than.json')
]

df = apply_functions(df, rules)
df.show()








