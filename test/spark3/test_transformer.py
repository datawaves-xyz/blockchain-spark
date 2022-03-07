from typing import AnyStr

from eth_abi import encode_abi
from pyspark.sql.types import StructType
from web3 import contract, Web3

import test
from test.spark3.utils import PySparkTestCase

RESOURCE_GROUP = 'ethereum_test'


def _get_resource_path(file_name: str) -> AnyStr:
    return test.get_resource_path([RESOURCE_GROUP], file_name)


def _read_resource(file_name: str) -> AnyStr:
    return test.read_resource([RESOURCE_GROUP], file_name)


class TransformerTestCase(PySparkTestCase):

    def test_parse_logs_to_function(self):

        log_abi = _read_resource('log_abi1.json')
        w3 = Web3()
        c = w3.eth.contract(abi=log_abi)
        print(c.encodeABI(fn_name='testCopyAddress', args=[Web3.toChecksumAddress('0x40b39bacd1658fffa015468ba88303f3d67a8740')]))

