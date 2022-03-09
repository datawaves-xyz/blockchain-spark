import unittest
import test.resources

from typing import AnyStr

from pyspark.sql.types import DataType, StructType

from spark3.ethereum.contract import get_call_schema_map
from spark3.types import ABIElement
from spark3.utils.abi import normalize_abi, filter_by_name

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


def get_abi_element_from_resource(file_name: str, name: str) -> ABIElement:
    return filter_by_name(name, normalize_abi(_read_resource(file_name)))[0]


class SchemaTestCase(unittest.TestCase):

    def test_get_function_call_schema(self):
        struct_type = get_call_schema_map(
            get_abi_element_from_resource('trace_abi1.json', 'acceptBid'),
        )

        self.assertEqual(deep_get_data_type('inputs.tokenId', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.bid', struct_type).typeName(), 'struct')
        self.assertEqual(deep_get_data_type('inputs.bid.amount', struct_type).typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.bid.currency', struct_type).typeName(), 'string')

    def test_address_parse_as_string(self):
        struct_type = get_call_schema_map(
            get_abi_element_from_resource('trace_abi1.json', 'isApprovedForAll')
        )
        self.assertEqual(deep_get_data_type('inputs.owner', struct_type).typeName(), 'string')

    def test_function_outputs(self):
        struct_type = get_call_schema_map(
            get_abi_element_from_resource('trace_abi1.json', 'MINT_WITH_SIG_TYPEHASH')
        )
        self.assertEqual(deep_get_data_type('outputs._0', struct_type).typeName(), 'binary')

    def test_get_function_call_schema2(self):
        struct_type = get_call_schema_map(
            get_abi_element_from_resource('log_abi2.json', 'AddLiquidity')
        )

        self.assertEqual(deep_get_data_type('inputs.provider', struct_type).typeName(), 'string')
        self.assertEqual(deep_get_data_type('inputs.token_amounts', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('inputs.token_amounts', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.fees', struct_type).typeName(), 'array')
        self.assertEqual(deep_get_data_type('inputs.fees', struct_type).elementType.typeName(), 'decimal')
        self.assertEqual(deep_get_data_type('inputs.invariant', struct_type).typeName(), 'decimal')
