import os
import logging
import warnings
import glob
import logging
import pyspark

from pyspark.sql import SparkSession

from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql import HiveContext

logger = logging.getLogger(__name__)


def quiet_py4j(sc, level=logging.WARN):
    """
    Reduce the amount of trace info produced by Spark's runtime
    In Spark 2.x we can configure it dynamically:
    `spark.sparkContext.setLogLevel('WARN')`
    """
    logging.getLogger('py4j').setLevel(logging.ERROR)

    logger = spark._sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("py4j").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def _external_libraries(path):
    """
    Returns a comma separated list of all of the *.jar packages found in 'path'
    """
    jars = ",".join(glob.glob("external-libraries/*.jar"))

    logger.debug('External jars:')
    for jar in jars:
        logger.debug(jar)

    return jars


def _default_spark_configuration():
    """
    Default configuration for spark's unit testing
    All of the external libraries (jars) area also made available to the spark application (set in 'spark.jars')
    """
    external_jars = _external_libraries('')

    conf = SparkConf()

    if external_jars:
        conf.set("spark.jars", external_jars)

    conf.set("spark.executor.memory", "1g")
    conf.set("spark.cores.max", "2")
    conf.set("spark.app.name", "sparkTestApp")

    return conf


def bootstrap_test_spark_session(conf=None):
    """
    Setup SparkContext
    """
    conf = conf or _default_spark_configuration()
    spark = SparkSession.builder.master("local").getOrCreate()
    spark._sc.setLogLevel('WARN')
    return spark


spark = bootstrap_test_spark_session()
quiet_py4j(spark)

