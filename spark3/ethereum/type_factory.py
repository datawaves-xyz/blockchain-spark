import re
from typing import List, Dict

from spark3.ethereum.abi_type import *
from spark3.ethereum.exceptions import TypeNotSupported


class TypeFactory:
    abi_types: List[ABIType] = [
        IntType(bit_length=256, unsigned=False, name='int'),
        IntType(bit_length=256, unsigned=True, name='uint'),
        AddressType(),
        BoolType(),
        FixedType(bit_length=256, scale=18, unsigned=False, name='fixed'),
        FixedType(bit_length=256, scale=18, unsigned=True, name='ufixed'),
        BytesType(name='bytes'),
        FunctionType(),
        StringType()
    ]

    for i in range(8, 257, 8):
        abi_types.append(IntType(bit_length=i, unsigned=False, name=f'int{i}'))
        abi_types.append(IntType(bit_length=i, unsigned=True, name=f'uint{i}'))

        for j in range(1, 81):
            abi_types.append(FixedType(bit_length=i, scale=j, unsigned=False, name=f'fixed{i}x{j}'))
            abi_types.append(FixedType(bit_length=i, scale=j, unsigned=True, name=f'ufixed{i}x{j}'))

    for i in range(1, 33):
        abi_types.append(BytesType(length=i, name=f'bytes{i}'))

    abi_types_map: Dict[str, ABIType] = {i.name: i for i in abi_types}

    @staticmethod
    def abi_type_to_spark_type(abi_type: str) -> DataType:
        array_reg = re.search(r'\[[\d]*\]$', abi_type)

        if array_reg:
            return ArrayType(TypeFactory.abi_type_to_spark_type(abi_type[:array_reg.start()]))
        elif abi_type in TypeFactory.abi_types_map:
            return TypeFactory.abi_types_map[abi_type].to_spark_type()
        else:
            raise TypeNotSupported(abi_type)
