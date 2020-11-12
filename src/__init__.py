import sys
sys.path.append('/Users/raulferreira/garuda/poc-dsl')

import pyspark

import pyspark.sql.functions as F
import pyspark.sql.types as T

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


