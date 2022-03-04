import re
import json
import functools

from typing import Dict, Any, List, Optional
from pyspark.sql.functions import col
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    LongType,
    IntegerType,
    DataType,
    StringType,
    BooleanType,
    ArrayType,
    BinaryType,
    DecimalType
)

from spark3.exceptions import (
    ColumnNotFoundInDataFrame,
    ContractABINotConfigured,
    FunctionOrEventNotInContractABI,
    TypeNotSupported
)

from spark3.providers import IContractABIProvider

# ABI types: https://docs.soliditylang.org/en/v0.8.11/abi-spec.html#types
abi_to_spark_type: Dict[str, DataType] = {
    r'^uint256$': DecimalType(precision=38),
    r'^int256$': DecimalType(precision=38),
    r'^uint128$': DecimalType(precision=38),
    r'^int128$': DecimalType(precision=38),
    r'^uint64$': LongType(),
    r'^int64$': LongType(),
    r'^uint32$': LongType(),
    r'^int32$': LongType(),
    r'^uint16$': IntegerType(),
    r'^int16$': IntegerType(),
    r'^uint8$': IntegerType(),
    r'^int8$': IntegerType(),
    r'^address$': StringType(),
    r'^bool$': BooleanType(),
    r'^fixed.*x[^\[\]]*$': DecimalType(),
    r'^ufixed.*x[^\[\]]*$': DecimalType(),
    r'^bytes[^\[\]]*$': BinaryType(),
    r'^string$': StringType()
}


class Contract:
    def __init__(self, spark3,
                 address: str,
                 abi: Optional[str] = None,
                 abi_provider: Optional[IContractABIProvider] = None):
        self.spark3 = spark3

        self.address = address
        self._abi_json = abi
        self._abi_provider = abi_provider
        if abi is None and abi_provider is None:
            raise ValueError("Either abi or abi_provider should be provided")

        self._function_schema = None
        self._event_schema = None

    @functools.cached_property
    def abi(self) -> List[Dict[str, Any]]:
        return json.loads(self.abi_json)

    @property
    def abi_json(self) -> str:
        if self._abi_json is not None:
            return self._abi_json

        if self._abi_provider is not None:
            return self._abi_provider.get_contract_abi(self.address)

        raise ContractABINotConfigured()

    @property
    def function_schema(self) -> Dict[str, StructType]:
        if self._function_schema is None:
            self._function_schema = get_function_schema(self.abi)
        return self._function_schema

    @property
    def event_schema(self) -> Dict[str, StructType]:
        if self._event_schema is None:
            self._event_schema = get_event_schema(self.abi)
        return self._event_schema

    def get_function_by_name(self, name: str) -> DataFrame:
        schema = self.function_schema.get(name)
        if schema is None:
            raise FunctionOrEventNotInContractABI()

        df = self.spark3.trace_df

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "to_address" and col_type == "string"]) != 1:
            raise ColumnNotFoundInDataFrame("to_address(string)", df)

        df = df.filter(col("to_address") == self.address)

        return self.spark3.transformer().parse_trace_to_function(
            df,
            self._get_abi_item_json(name),
            schema,
            name
        )

    def get_event_by_name(self, name: str) -> DataFrame:
        schema = self.event_schema.get(name)
        if schema is None:
            raise FunctionOrEventNotInContractABI()

        df = self.spark3.log_df

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "address" and col_type == "string"]) != 1:
            raise ColumnNotFoundInDataFrame("address(string)", df)

        df = df.filter(col("address") == self.address)

        return self.spark3.transformer().parse_log_to_event(
            df,
            self._get_abi_item_json(name),
            schema,
            name
        )

    def _get_abi_item_json(self, name):
        abi_list = [x for x in self.abi
                    if x['type'] in ('function', 'event') and x['name'] == name]
        if len(abi_list) == 0:
            raise FunctionOrEventNotInContractABI()
        return json.dumps(abi_list[0])

    @functools.cached_property
    def all_functions(self) -> Dict[str, DataFrame]:
        return {name: self.get_function_by_name(name) for name in self.function_schema.keys()}

    @functools.cached_property
    def all_events(self) -> Dict[str, DataFrame]:
        return {name: self.get_event_by_name(name) for name in self.event_schema.keys()}


def _get_spark_type_by_name(type_name: str) -> DataType:
    for regex_str, data_type in abi_to_spark_type.items():
        if re.search(regex_str, type_name) is not None:
            return data_type

    array_reg = re.search(r'\[[\d]*\]$', type_name)
    if array_reg:
        return ArrayType(_get_spark_type_by_name(type_name[:array_reg.start()]))
    else:
        raise TypeNotSupported(type_name)


def _flatten_schema_from_components(component_abi: List[Dict[str, Any]]) -> StructType:
    struct_type = StructType()
    for idx, field in enumerate(component_abi):
        # the default name for anonymous field is '_{idx}'
        fname = field.get('name') if len(field.get('name', '')) > 0 else f'_{idx}'
        ftype = field.get('type')
        if ftype != 'tuple':
            struct_type.add(field=fname, data_type=_get_spark_type_by_name(ftype),
                            metadata={'type': ftype})
        else:
            struct_type.add(
                field=fname,
                data_type=_flatten_schema_from_components(component_abi=field.get('components')),
                metadata={'type': ftype},
            )

    return struct_type


def get_call_schema_map(abi: List[Dict[str, Any]], mode: str) -> Dict:
    assert (mode == 'function' or mode == 'event')

    func_abi_list = [i for i in abi if i.get('type') == mode]
    schemas_by_func_name = {}
    for func_abi in func_abi_list:
        func_name = func_abi.get('name')

        s = StructType()

        if len(func_abi.get('inputs', [])) > 0:
            s.add(field='inputs',
                  data_type=_flatten_schema_from_components(component_abi=func_abi.get('inputs')))

        if len(func_abi.get('outputs', [])) > 0:
            s.add(field='outputs',
                  data_type=_flatten_schema_from_components(component_abi=func_abi.get('outputs')))

        schemas_by_func_name[func_name] = s
    return schemas_by_func_name


def get_function_schema(abi: List[Dict[str, Any]]) -> Dict[str, StructType]:
    return get_call_schema_map(abi, 'function')


def get_event_schema(abi: List[Dict[str, Any]]) -> Dict[str, StructType]:
    return get_call_schema_map(abi, 'event')
