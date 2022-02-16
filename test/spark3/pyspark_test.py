import unittest

import pyspark


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = pyspark.SparkConf() \
            .setMaster("local[*]") \
            .setAppName("testing") \
            .set("spark.driver.bindAddress", "127.0.0.1")

        cls.sc = pyspark.SparkContext(conf=conf)
        cls.spark = pyspark.SQLContext(cls.sc)

    @classmethod
    def tearDownClass(cls):
        cls.sc.stop()
