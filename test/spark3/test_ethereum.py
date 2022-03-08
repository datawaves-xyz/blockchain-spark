import json
import unittest
from typing import AnyStr

from pyspark.sql.types import DataType, StructType

import test.resources
from spark3.ethereum.contract import get_function_schema, get_event_schema

RESOURCE_GROUP = 'ethereum_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


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

        self.assertEqual(deep_get_data_type('inputs.tokenId', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.bid', struct_type).typeName(), 'struct')
        self.assertEqual(deep_get_data_type('inputs.bid.amount', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.bid.currency', struct_type).typeName(), 'string')

    def test_address_parse_as_string(self):
        abi = json.loads(_read_resource('trace_abi1.json'))
        struct_type = get_function_schema(abi=abi)['isApprovedForAll']
        self.assertEqual(deep_get_data_type('inputs.owner', struct_type).typeName(), 'string')

    def test_function_outputs(self):
        abi = json.loads(_read_resource('trace_abi1.json'))
        schema_map = get_function_schema(abi=abi)
        struct_type = schema_map['MINT_WITH_SIG_TYPEHASH']
        print(struct_type)
        self.assertEqual(deep_get_data_type('outputs._0', struct_type).typeName(), 'binary')

    # TODO est complex ABI type
    def test_get_function_call_schema2(self):
        abi = json.loads(_read_resource('log_abi2.json'))
        struct_type = get_event_schema(abi=abi)['AddLiquidity']

        self.assertEqual(deep_get_data_type('inputs.provider', struct_type).typeName(), 'string')
        self.assertEqual(deep_get_data_type('inputs.token_amounts', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('inputs.token_amounts', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.fees', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('inputs.fees', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.invariant', struct_type).typeName(), 'decimal')
