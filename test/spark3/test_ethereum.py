import json
import unittest
from typing import AnyStr

from pyspark.sql.types import DataType, StructType

import test.resources
from spark3.ethereum import get_function_schema, get_event_schema

RESOURCE_GROUP = 'ethereum_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.resources.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.resources.read_resource([RESOURCE_GROUP], file_name)


def deep_get_data_type(path: str, dtype: StructType) -> DataType:
    split_path = path.split('.')
    item_type = dtype[split_path[0]].dataType
    return item_type if len(split_path) == 1 else \
        deep_get_data_type('.'.join(split_path[1:]), item_type)


class SchemaTestCase(unittest.TestCase):

    def test_get_function_call_schema(self):
        abi = json.loads(_read_resource('trace_abi1.json'))
        schema_map = get_function_schema(abi=abi)
        struct_type = schema_map['acceptBid']

        self.assertEqual(deep_get_data_type('tokenId', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('bid', struct_type).typeName(), 'struct')
        self.assertEqual(deep_get_data_type('bid.amount', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('bid.currency', struct_type).typeName(), 'string')

    def test_address_parse_as_string(self):
        abi = json.loads(_read_resource('trace_abi1.json'))
        struct_type = get_function_schema(abi=abi)['isApprovedForAll']
        self.assertEqual(deep_get_data_type('owner', struct_type).typeName(), 'string')

    def test_empty_function_inputs(self):
        abi = json.loads(_read_resource('trace_abi1.json'))
        schema_map = get_function_schema(abi=abi)
        self.assertEqual(schema_map['MINT_WITH_SIG_TYPEHASH'].names, [])
        self.assertEqual(schema_map['PERMIT_TYPEHASH'].names, [])

    # TODO est complex ABI type
    def test_get_function_call_schema2(self):
        abi = json.loads(_read_resource('log_abi2.json'))
        struct_type = get_event_schema(abi=abi)['AddLiquidity']

        self.assertEqual(deep_get_data_type('provider', struct_type).typeName(), 'string')
        self.assertEqual(deep_get_data_type('token_amounts', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('token_amounts', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('fees', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('fees', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('invariant', struct_type).typeName(), 'decimal')
