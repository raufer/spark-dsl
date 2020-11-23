import os
import logging
import warnings

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")

logging.basicConfig(
    format='%(asctime)s, %(name)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG
)

ROOT = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))



