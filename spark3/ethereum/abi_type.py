import math
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

        # Ignore ByteType(8bit) and ShortType(16bit) for match the type in the headlong.
        if 0 < signed_bit_length <= 32:
            return IntegerType()
        elif 32 < signed_bit_length <= 64:
            return LongType()
        elif 64 < signed_bit_length:
            return DecimalType(38, 0)


class StringType(ABIType):
    def __init__(self, name: str = 'string'):
        super().__init__(canonical_type='string', name=name)

    def to_spark_type(self) -> DataType:
        return SparkStringType()


class AddressType(StringType):
    """
    The address type equivalent to uint160 in the document, but we will transform to HexString to show.
    """

    def __init__(self):
        super(AddressType, self).__init__(name='address')


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
        # precision = log10(2^bit_length) + 1
        # The precision can be up to 38, scale can also be up to 38 (less or equal to precision) in Spark
        # doc: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/DecimalType.html
        precision = min(38, math.floor(signed_bit_length / math.log2(10)) + 1)
        return DecimalType(precision=min(38, math.floor(signed_bit_length / math.log2(10)) + 1),
                           scale=min(precision, self.scale))


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
