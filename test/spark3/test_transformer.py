import test.resources
import pyspark

from decimal import Decimal

from spark3 import Spark3
from test.spark3.utils import PySparkTestCase

RESOURCE_GROUP = ['transformer_test']


class TransformerTestCase(PySparkTestCase):
    def test_function_input_can_decode(self):
        abi = test.read_resource(RESOURCE_GROUP, 'opensea_abi.json')
        spark = pyspark.SQLContext(self.sc)
        trace_df = spark.read.option('multiline', 'true') \
            .json(test.get_resource_path(RESOURCE_GROUP, 'opensea_trace.json'))
        trace_df.show()

        spark3 = Spark3(self.sc, trace_df, trace_df)
        contract = spark3.contract(address='', abi=abi)
        func_df = contract.get_function_by_name('atomicMatch_')
        results = func_df.collect()
        inputs = results[0]['function_parameter']['inputs']

        self.assertEqual(14, len(inputs['addrs']))
        self.assertIsInstance(inputs['addrs'][0], str)

        self.assertEqual(18, len(inputs['uints']))
        self.assertIsInstance(inputs['uints'][0], Decimal)

        self.assertEqual(8, len(inputs['feeMethodsSidesKindsHowToCalls']))
        self.assertIsInstance(inputs['feeMethodsSidesKindsHowToCalls'][0], int)

        self.assertIsInstance(inputs['calldataBuy'], bytearray)
        self.assertIsInstance(inputs['calldataSell'], bytearray)
        self.assertIsInstance(inputs['replacementPatternBuy'], bytearray)
        self.assertIsInstance(inputs['replacementPatternSell'], bytearray)
        self.assertIsInstance(inputs['staticExtradataSell'], bytearray)
        self.assertIsInstance(inputs['staticExtradataSell'], bytearray)

        self.assertEqual(28, inputs['vs'][0])
        self.assertIsInstance(inputs['vs'][0], int)

        self.assertEqual(5, len(inputs['rssMetadata']))
        self.assertIsInstance(inputs['rssMetadata'][0], bytearray)

