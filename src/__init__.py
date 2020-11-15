import os
import logging
import warnings
import pyspark

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")

logging.basicConfig(
    format='%(asctime)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.INFO
)

ROOT = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

spark = SparkSession.builder.master("local").getOrCreate()


