from decimal import Decimal
from typing import AnyStr

from pyspark.sql.types import StructType, StructField, StringType
from web3 import Web3

import test
from spark3 import Spark3
from test.spark3.utils import PySparkTestCase

RESOURCE_GROUP = 'transform_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


mock_contract_address = "0x4aB190Bc3c60678705Cd819a37E10Af7417AC89B"
mock_address = "0x40b39bacd1658fffa015468ba88303f3d67a8740"


class TransformerTestCase(PySparkTestCase):

    def test_parse_logs_to_function(self):
        # Mock traces dataframe
        w3 = Web3()
        abi_str = _read_resource('log_abi1.json')
        c = w3.eth.contract(abi=abi_str)
        data = c.encodeABI(
            fn_name='AllTypeFunction',
            args=[
                Web3.toChecksumAddress(mock_address),
                -100,
                -30000,
                -2000000000,
                -8000000000,
                100,
                30000,
                2000000000,
                8000000000,
                True,
                Decimal('-10000.2000'),
                Decimal('1000.2000'),
                bytes('hello world', 'UTF-8'),
                'This is a test',
                [1, 2, 3, 4, 5, -8],
            ])
        df = self.sc \
            .parallelize([(data, "", mock_contract_address)]) \
            .toDF(schema=StructType([StructField("input", StringType()),
                                     StructField("output", StringType()),
                                     StructField("to_address", StringType())]))

        spark3 = Spark3(spark=self.sql.sparkSession, trace=df, log=None)
        contract = spark3.contract(address=mock_contract_address, abi=abi_str)
        new_df = contract.get_function_by_name("AllTypeFunction")

        input = new_df.collect()[0]['function_parameter']['inputs']
        self.assertEqual(input['addr'], mock_address)
        self.assertEqual(input['i8'], -100)
        self.assertEqual(input['i16'], -30000)
        self.assertEqual(input['i32'], -2000000000)
        self.assertEqual(input['i64'], -8000000000)
        self.assertEqual(input['ui8'], 100)
        self.assertEqual(input['ui16'], 30000)
        self.assertEqual(input['ui32'], 2000000000)
        self.assertEqual(input['ui64'], 8000000000)
        self.assertTrue(input['bool'])
        self.assertTrue(Decimal('-10000.20000000000000000000').compare(input['f128x20']) == 0)
        self.assertTrue(Decimal('1000.20000000000000000000').compare(input['uf128x20']) == 0)
        self.assertEqual(input['bytes'], bytes('hello world', 'UTF-8'))
        self.assertEqual(input['str'], 'This is a test')
        self.assertEqual(input['int8_array'], [1, 2, 3, 4, 5, -8])

        input_schema = {field['name']: field['type'] for field in
                        new_df.schema.fields[1].dataType.jsonValue()['fields'][0]['type']['fields']}
        self.assertEqual(input_schema['addr'], 'string')
        self.assertEqual(input_schema['i8'], 'integer')
        self.assertEqual(input_schema['i16'], 'integer')
        self.assertEqual(input_schema['i32'], 'integer')
        self.assertEqual(input_schema['i64'], 'long')
        self.assertEqual(input_schema['ui8'], 'integer')
        self.assertEqual(input_schema['ui16'], 'integer')
        self.assertEqual(input_schema['ui32'], 'long')
        self.assertEqual(input_schema['ui64'], 'decimal(38,0)')
        self.assertEqual(input_schema['bool'], 'boolean')
        self.assertEqual(input_schema['f128x20'], 'decimal(38,20)')
        self.assertEqual(input_schema['uf128x20'], 'decimal(38,20)')
        self.assertEqual(input_schema['bytes'], 'binary')
        self.assertEqual(input_schema['str'], 'string')
        self.assertEqual(input_schema['int8_array']['type'], 'array')
        self.assertEqual(input_schema['int8_array']['elementType'], 'integer')
