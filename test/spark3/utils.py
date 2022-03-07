import unittest
import logging

from pyspark.context import SparkContext
from pyspark.conf import SparkConf

# Make py4j quiet
logger = logging.getLogger('py4j')
logger.setLevel(logging.INFO)


class PySparkTestCase(unittest.TestCase):
    def setUp(self):
        class_name = self.__class__.__name__
        conf = SparkConf()
        self.sc = SparkContext('local', class_name, conf=conf)

    def tearDown(self):
        self.sc.stop()
