import logging
import unittest

from pyspark import SQLContext
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# Make py4j quiet

logger = logging.getLogger('py4j')
logger.setLevel(logging.INFO)


class PySparkTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf() \
            .set("spark.driver.host", "127.0.0.1") \
            .set("spark.driver.bindAddress", "127.0.0.1")

        self.sc = SparkContext(master='local[*]', appName=class_name, conf=conf)
        self.sql = SQLContext(self.sc)

    def tearDown(self):
        self.sc.stop()
