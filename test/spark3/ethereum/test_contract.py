import json
from decimal import Decimal
from typing import AnyStr

from eth_utils import function_abi_to_4byte_selector, encode_hex
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from web3 import Web3

import test
from datawaves import new_trace_conditions
from datawaves.utils import hash_unsafe_bytes
from spark3 import Spark3
from test.spark3._utils import PySparkTestCase

RESOURCE_GROUP = 'contract_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


mock_contract_address = "0x4aB190Bc3c60678705Cd819a37E10Af7417AC89B"
mock_address = "0x40b39bacd1658fffa015468ba88303f3d67a8740"


class ContractTestCase(PySparkTestCase):

    def test_get_function_by_name(self):
        # Mock traces dataframe
        w3 = Web3()
        abi_str = _read_resource('abi1.json')
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
                bytes('hi', 'UTF-8'),
                'This is a test',
                [1, 2, 3, 4, 5, -8],
                [bytes('test1', 'UTF-8'), bytes('test2', 'UTF-8')],
                [True, True, True, True, True, False, False, False, False, True],
                {'key': 'test', 'value': 1234}
            ])

        function_abi = json.loads(abi_str)[0]
        selector = encode_hex(function_abi_to_4byte_selector(function_abi))

        df = self.sc \
            .parallelize([(data,
                           "",
                           mock_contract_address,
                           abs(hash_unsafe_bytes(mock_contract_address)) % 10,
                           selector,
                           abs(hash_unsafe_bytes(selector)) % 10)]) \
            .toDF(schema=StructType([StructField("input", StringType()),
                                     StructField("output", StringType()),
                                     StructField("to_address", StringType()),
                                     StructField("address_hash", IntegerType()),
                                     StructField("selector", StringType()),
                                     StructField("selector_hash", IntegerType())]))

        spark3 = Spark3(
            spark=self.sql.sparkSession,
            trace=df,
            trace_conditions=new_trace_conditions()
        )
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
        self.assertEqual(input['bytes10'], bytes('hi\x00\x00\x00\x00\x00\x00\x00\x00', 'UTF-8'))
        self.assertEqual(input['str'], 'This is a test')
        self.assertEqual(input['int8_array'], [1, 2, 3, 4, 5, -8])
        self.assertEqual(input['bytes12_array'], [bytes('test1\x00\x00\x00\x00\x00\x00\x00', 'UTF-8'),
                                                  bytes('test2\x00\x00\x00\x00\x00\x00\x00', 'UTF-8')])
        self.assertEqual(input['bool_array'], [True, True, True, True, True, False, False, False, False, True])
        self.assertEqual(input['tuple']['key'], 'test')
        self.assertEqual(input['tuple']['value'], 1234)

        input_schema_field = [field for field in new_df.schema.fields if field.name == 'function_parameter'][0]
        input_schema = {field['name']: field['type'] for field in
                        input_schema_field.dataType.jsonValue()['fields'][0]['type']['fields']}
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
        self.assertEqual(input_schema['bytes10'], 'binary')
        self.assertEqual(input_schema['str'], 'string')
        self.assertEqual(input_schema['int8_array']['type'], 'array')
        self.assertEqual(input_schema['int8_array']['elementType'], 'integer')
        self.assertEqual(input_schema['bytes12_array']['type'], 'array')
        self.assertEqual(input_schema['bytes12_array']['elementType'], 'binary')
        self.assertEqual(input_schema['bool_array']['type'], 'array')
        self.assertEqual(input_schema['bool_array']['elementType'], 'boolean')

        tuple_schema = {i['name']: i['type'] for i in input_schema['tuple']['fields']}
        self.assertEqual(tuple_schema['key'], 'string')
        self.assertEqual(tuple_schema['value'], 'decimal(38,0)')
