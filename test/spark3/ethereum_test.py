import json
import unittest
from typing import AnyStr

from pyspark.sql.types import DataType, StructType

import test.resources
from spark3.ethereum import decode_function_input_with_abi, get_call_schema, decode_log_data_with_abi

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
        struct_type = get_call_schema(abi=abi, mode='function')

        # MINT_WITH_SIG_TYPEHASH has no inputs
        self.assertFalse('MINT_WITH_SIG_TYPEHASH' in struct_type.fieldNames())
        # PERMIT_TYPEHASH has no inputs
        self.assertFalse('PERMIT_TYPEHASH' in struct_type.fieldNames())
        self.assertTrue('getApproved' in struct_type.fieldNames())

        self.assertTrue(deep_get_data_type('acceptBid', struct_type).typeName(), 'struct')
        self.assertTrue(deep_get_data_type('acceptBid.tokenId', struct_type).typeName(), 'long')
        self.assertTrue(deep_get_data_type('acceptBid.bid', struct_type).typeName(), 'struct')
        self.assertTrue(deep_get_data_type('acceptBid.bid.amount', struct_type).typeName(), 'long')
        self.assertTrue(deep_get_data_type('acceptBid.bid.currency', struct_type).typeName(), 'string')

        # TODO test complex ABI type

    def test_decode_function_input_with_abi(self):
        abi = _read_resource('trace_abi1.json')
        data = _read_resource('trace_input1.txt')
        input_data = decode_function_input_with_abi(input_data=data, abi_str=abi)

        self.assertEqual(
            input_data['mint']['data']['tokenURI'],
            'https://ipfs.fleek.co/ipfs/bafybeifyqibqlheu7ij7fwdex4y2pw2wo7eaw2z6lec5zhbxu3cvxul6h4')
        self.assertEqual(
            input_data['mint']['data']['metadataHash'],
            b'\x9e\xf9\xd6\xd1\xdc<\xfb\xd36T\x95\x078l@36C0Puv_d-\xb4\x1f\xb5v\xf8\xf9\x9d')
        self.assertEqual(input_data['mint']['bidShares']['prevOwner']['value'], 0)
        self.assertEqual(input_data['mint']['bidShares']['owner']['value'], 100000000000000000000)

        # TODO test complex ABI type data

    def test_decode_log_data_with_abi(self):
        abi = _read_resource('log_abi1.json')
        log = json.loads(_read_resource('log1.json'))
        log_data = decode_log_data_with_abi(data=log.get('data'), topics_str=log.get('topics'), abi_str=abi)

        self.assertEqual(log_data['OrdersMatched']['maker'], '0x6Be4a7bbb812Bfa6A63126EE7b76C8A13529BDB8')
        self.assertEqual(log_data['OrdersMatched']['taker'], '0x0239769A1aDF4DeF9f07Da824B80B9C4fCB59593')
        self.assertEqual(
            log_data['OrdersMatched']['metadata'],
            b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00')
        self.assertEqual(
            log_data['OrdersMatched']['price'],
            2000000000000000
        )

        # TODO test complex ABI type data
