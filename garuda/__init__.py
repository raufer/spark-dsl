import os
import logging
import warnings

from pyspark.sql import SparkSession

warnings.filterwarnings("ignore")

file_format ='%(asctime)s, %(name)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
console_format ='%(asctime)s, %(name)s, %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'

ROOT = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

logging.basicConfig(
    format=file_format,
    datefmt=console_format,
    level=logging.INFO,
    filename=os.path.join(ROOT, 'dq.log'),
    filemode='w'
)

formatter = logging.Formatter(console_format)
console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)




