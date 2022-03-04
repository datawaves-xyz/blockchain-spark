from typing import Optional

from pyspark.sql.types import *
from pyspark.sql.types import StringType as SparkStringType

from spark3.ethereum.exceptions import ABITypeNotValid


class ABIType:
    """
    Follow by: https://docs.soliditylang.org/en/v0.8.11/abi-spec.html#types
    """

    def __init__(self, canonical_type: str, name: str):
        self.canonical_type = canonical_type
        self.name = name

    def to_spark_type(self) -> DataType:
        raise Exception('The function must be implemented.')


class IntType(ABIType):
    def __init__(self, bit_length: int, unsigned: bool, *args, **kwargs):
        if bit_length > 256 or bit_length <= 0 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the integer type must less than or equal to 256, more than 0 and divisible by 8.')

        super(IntType, self).__init__(
            canonical_type=f'{"u" if unsigned else ""}int{bit_length}',
            *args, **kwargs
        )
        self.bit_length = bit_length
        self.unsigned = unsigned

    def to_spark_type(self) -> DataType:
        signed_bit_length = self.bit_length + self.unsigned

        if 0 < signed_bit_length <= 8:
            return ByteType()
        elif 8 < signed_bit_length <= 16:
            return ShortType()
        elif 16 < signed_bit_length <= 32:
            return IntegerType()
        else:
            return LongType()


class AddressType(IntType):
    def __init__(self):
        super(AddressType, self).__init__(bit_length=160, unsigned=True, name='address')


class BoolType(ABIType):
    def __init__(self):
        super(BoolType, self).__init__(canonical_type='bool', name='bool')

    def to_spark_type(self) -> DataType:
        return BooleanType()


class FixedType(ABIType):
    def __init__(self, bit_length: int, scale: int, unsigned: bool, *args, **kwargs):
        if bit_length > 256 or bit_length < 8 or bit_length % 8 != 0:
            raise ABITypeNotValid(
                'The bit length of the fixed type must less than or equal to 256, more than 0 and divisible by 8.')

        if scale > 80 or scale <= 0:
            raise ABITypeNotValid(
                'The scale of the fixed type must less than or equal to 80 and more than 0.')

        super(FixedType, self).__init__(
            canonical_type=f'{"u" if unsigned else ""}fixed{bit_length}x{scale}',
            *args, **kwargs
        )
        self.bit_length = bit_length
        self.scale = scale
        self.unsigned = unsigned

    def to_spark_type(self) -> DataType:
        signed_bit_length = self.bit_length + self.unsigned
        return DecimalType(precision=signed_bit_length, scale=self.scale)


class BytesType(ABIType):
    def __init__(self, length: Optional[int] = None, *args, **kwargs):
        if length is not None:
            if length > 32 or length <= 0:
                raise ABITypeNotValid(
                    'The length of bytes must les than or equal to 32 and more than 0.')

        super().__init__(
            canonical_type=f'bytes{"" if length is None else length}',
            *args, **kwargs
        )

        if length is not None:
            self.length = length

    def to_spark_type(self) -> DataType:
        return BinaryType()


class FunctionType(BytesType):
    def __init__(self):
        # an address(20 bytes) followed by a function selector(4 bytes)
        super(FunctionType, self).__init__(length=24, name='function')


class StringType(ABIType):
    def __init__(self):
        super().__init__(canonical_type='string', name='string')

    def to_spark_type(self) -> DataType:
        return SparkStringType()
