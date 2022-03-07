import unittest
from typing import Dict

from pyspark.sql.types import *

from spark3.ethereum.type_factory import TypeFactory


class TypeFactoryTestCase(unittest.TestCase):
    def test_int_type(self):
        test_type_map: Dict[str, DataType] = {
            'int': DecimalType(38, 0),
            'uint': DecimalType(38, 0),
            'int8': IntegerType(),
            'uint8': IntegerType(),
            'int16': IntegerType(),
            'uint16': IntegerType(),
            'int32': IntegerType(),
            'uint32': LongType(),
            'int248': DecimalType(38, 0),
            'uint248': DecimalType(38, 0)
        }

        for atype, stype in test_type_map.items():
            self.assertEqual(TypeFactory.abi_type_to_spark_type(atype), stype)

    def test_fixed_type(self):
        test_type_map: Dict[str, DataType] = {
            'fixed': DecimalType(38, 18),
            'ufixed': DecimalType(38, 18),
            'fixed8x20': DecimalType(3, 3),
            'ufixed8x20': DecimalType(3, 3),
        }

        for atype, stype in test_type_map.items():
            self.assertEqual(TypeFactory.abi_type_to_spark_type(atype), stype)

    def test_other_base_type(self):
        test_type_map: Dict[str, DataType] = {
            'address': StringType(),
            'bool': BooleanType(),
            'bytes': BinaryType(),
            'bytes30': BinaryType(),
            'function': BinaryType(),
            'string': StringType(),
        }

        for atype, stype in test_type_map.items():
            self.assertEqual(TypeFactory.abi_type_to_spark_type(atype), stype)

    def test_array_type(self):
        test_type_map: Dict[str, DataType] = {
            'uint32[2]': ArrayType(LongType()),
            'fixed80x50[10]': ArrayType(DecimalType(25, 25)),
            'string[5]': ArrayType(StringType()),
            'bytes20[7]': ArrayType(BinaryType())
        }

        for atype, stype in test_type_map.items():
            self.assertEqual(TypeFactory.abi_type_to_spark_type(atype), stype)
