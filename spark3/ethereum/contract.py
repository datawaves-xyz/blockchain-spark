import functools
import json
from typing import Sequence, Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spark3.ethereum.type_factory import TypeFactory
from spark3.exceptions import (
    ContractABINotConfigured,
    ABIFunctionNotFound,
    ABIEventNotFound
)
from spark3.providers import IContractABIProvider
from spark3.types import ABI
from spark3.types import ABIElement, ABIFunctionElement
from spark3.utils.abi import normalize_abi, filter_by_type, filter_by_name


class Contract:
    def __init__(self, spark3,
                 address: str,
                 abi: Optional[str] = None,
                 abi_provider: Optional[IContractABIProvider] = None):
        self.spark3 = spark3
        self.address = address
        self._abi_provider = abi_provider
        self._abi_json = abi

        if self._abi_provider is not None and self._abi_json is None:
            # Getting abi json from abi provider only if abi is not provided
            self._abi_json = self._abi_provider.get_contract_abi(self.address)

        if self._abi_json is None:
            raise ContractABINotConfigured("Either an ABI or an ABI provider should be provided")

        self._abi = normalize_abi(self._abi_json)
        self._function_schema = None
        self._event_schema = None

    def abi(self) -> ABI:
        return self._abi

    def abi_json(self) -> str:
        return self._abi_json

    @property
    def function_schema(self) -> Dict[str, StructType]:
        if self._function_schema is None:
            self._function_schema = {x.get('name'): get_call_schema_map(x)
                                     for x in filter_by_type('function', self._abi)}
        return self._function_schema

    @property
    def event_schema(self) -> Dict[str, StructType]:
        if self._event_schema is None:
            self._event_schema = {x.get('name'): get_call_schema_map(x)
                                  for x in filter_by_type('event', self._abi)}
        return self._event_schema

    def get_traces(self) -> DataFrame:
        if self.spark3.trace_df is None or self.spark3.trace_conditions is None:
            raise ValueError('Could not call get_traces without trace dataframe or trace conditions')

        return self.spark3.trace_conditions.act(self.spark3.trace_df, self.address)

    def get_logs(self) -> DataFrame:
        if self.spark3.log_df is None or self.spark3.log_conditions is None:
            raise ValueError('Could not call get_log without log dataframe or log conditions')

        return self.spark3.log_conditions.act(self.spark3.log_df, self.address)

    def get_function_by_name(self, name: str) -> DataFrame:
        if self.spark3.trace_df is None or self.spark3.trace_conditions is None:
            raise ValueError('Could not call get_function_by_name without trace dataframe or trace conditions')

        abi_list = filter_by_name(name, filter_by_type('function', self._abi))
        if len(abi_list) == 0:
            raise ABIFunctionNotFound('{} not found in ABI'.format(name))
        function_abi = abi_list[0]
        schema = get_call_schema_map(function_abi)

        df = self.spark3.trace_conditions \
            .act(self.spark3.trace_df, self.address, function_abi)

        return self.spark3.transformer() \
            .parse_trace_to_function(df, json.dumps(function_abi), schema, name, self.spark3.ignore_malformed)

    def get_event_by_name(self, name: str) -> DataFrame:
        if self.spark3.log_df is None or self.spark3.log_conditions is None:
            raise ValueError('Could not call get_event_by_name without log dataframe or log conditions')

        abi_list = filter_by_name(name, filter_by_type('event', self._abi))
        if len(abi_list) == 0:
            raise ABIEventNotFound('{} not found in ABI'.format(name))
        event_abi = abi_list[0]
        schema = get_call_schema_map(event_abi)

        df = self.spark3.log_conditions \
            .act(self.spark3.log_df, self.address, event_abi)

        return self.spark3.transformer() \
            .parse_log_to_event(df, json.dumps(event_abi), schema, name, self.spark3.ignore_malformed)

    @functools.cached_property
    def all_functions(self) -> Dict[str, DataFrame]:
        return {name: self.get_function_by_name(name) for name in self.function_schema.keys()}

    @functools.cached_property
    def all_events(self) -> Dict[str, DataFrame]:
        return {name: self.get_event_by_name(name) for name in self.event_schema.keys()}


def _flatten_schema_from_components(component_abi: Sequence[ABIFunctionElement]) -> StructType:
    struct_type = StructType()
    for idx, field in enumerate(component_abi):
        # the default name for anonymous field is '_{idx}'
        fname = field.get('name') if len(field.get('name', '')) > 0 else f'_{idx}'
        ftype = field.get('type')
        if ftype != 'tuple':
            struct_type.add(field=fname,
                            data_type=TypeFactory.abi_type_to_spark_type(ftype),
                            metadata={'type': ftype})
        else:
            struct_type.add(field=fname,
                            data_type=_flatten_schema_from_components(component_abi=field.get('components')),
                            metadata={'type': ftype})

    return struct_type


def get_call_schema_map(element: ABIElement) -> StructType:
    """Ethereum schema helper for converting :class:`ABIElement` to :class:`StructType`"""

    struct_type = StructType()

    if len(element.get('inputs', [])) > 0:
        struct_type.add(field='inputs',
                        data_type=_flatten_schema_from_components(component_abi=element.get('inputs')))

    if len(element.get('outputs', [])) > 0:
        struct_type.add(field='outputs',
                        data_type=_flatten_schema_from_components(component_abi=element.get('outputs')))
    return struct_type
