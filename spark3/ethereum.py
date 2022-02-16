import json
import re
from typing import Dict, Any, Tuple, List, Optional

from eth_utils import encode_hex, event_abi_to_log_topic, decode_hex
from pyspark.sql.types import StructType, LongType, DataType, StringType, DoubleType, BooleanType, ArrayType
from web3 import Web3

# ABI types: https://docs.soliditylang.org/en/v0.8.11/abi-spec.html#types
abi_to_spark_type: Dict[str, DataType] = {
    r'^uint[^\[\]]*$': LongType(),
    r'^int[^\[\]]*$': LongType(),
    r'^address$': StringType(),
    r'^bool$': BooleanType(),
    r'^fixed.*x[^\[\]]*$': DoubleType(),
    r'^ufixed.*x[^\[\]]*$': DoubleType(),
    r'^bytes[^\[\]]*$': StringType(),
    r'^string$': StringType()
}


def _get_spark_type_by_name(type_name: str) -> DataType:
    for regex_str, data_type in abi_to_spark_type.items():
        if re.search(regex_str, type_name) is not None:
            return data_type

        if type_name.endswith('[]'):
            return ArrayType(_get_spark_type_by_name(regex_str[:-2]))


def _flatten_schema_from_components(component_abi: [Dict[str, Any]]) -> StructType:
    struct_type = StructType()
    for field in component_abi:
        # the default name of filed is 'param'
        fname = field.get('name') if len(field.get('name', '')) > 0 else 'param'
        ftype = field.get('type')
        if ftype != 'tuple':
            struct_type.add(field=fname, data_type=_get_spark_type_by_name(ftype))
        else:
            struct_type.add(
                field=fname,
                data_type=_flatten_schema_from_components(component_abi=field.get('components'))
            )

    return struct_type


def get_call_schema(abi: [Dict[str, Any]], mode: str) -> StructType:
    assert (mode == 'function' or mode == 'event')

    struct_type = StructType()
    func_abi_list = [i for i in abi if i.get('type') == mode]

    for func_abi in func_abi_list:
        ftype = func_abi.get('inputs', [])
        if len(ftype) == 0:
            continue

        struct_type.add(
            field=func_abi.get('name'),
            data_type=_flatten_schema_from_components(component_abi=func_abi.get('inputs'))
        )

    return struct_type


def _flatten_tuple(raw_tuple: Tuple, schema: List[Dict[str, Any]]) -> Dict[str, Any]:
    result_dict = {}

    for index, field_type in enumerate(schema):
        fname = field_type.get('name')
        if field_type.get('type') != 'tuple':
            result_dict[fname] = raw_tuple[index]
        else:
            result_dict[fname] = _flatten_tuple(raw_tuple=raw_tuple[index],
                                                schema=field_type.get('components'))

    return result_dict


def _flatten_params(params: Dict[str, Any], schema: List[Dict[str, Any]]) -> Dict[str, Any]:
    result_dict = {}
    for key, value in params.items():
        fname = key if len(key) > 0 else 'param'

        if type(value) is not tuple:
            result_dict[fname] = value
        else:
            component_schema = [i for i in schema if i.get('name') == key and i.get('type') == 'tuple'][0] \
                .get('components')

            result_dict[fname] = _flatten_tuple(raw_tuple=value,
                                                schema=component_schema)

    return result_dict


def decode_function_input_with_abi(input_data: str, abi_str: str) -> Optional[Dict[str, Any]]:
    try:
        w3 = Web3()
        abi = json.loads(abi_str)
        contract = w3.eth.contract(abi=abi)
        func_obj, input_params = contract.decode_function_input(input_data)
        func_name = vars(func_obj)['fn_name']
        input_schema = [i for i in abi if i.get('name') == func_name and i.get('type') == 'function'][0].get('inputs')
        return {func_name: _flatten_params(params=input_params, schema=input_schema)}
    except Exception as ex:
        print(f'parsing input with abi failed: {str(ex)}')
        return None


def decode_log_data_with_abi(data: str, topics_str: str, abi_str: str) -> Optional[Dict[str, Any]]:
    try:
        w3 = Web3()
        abi = json.loads(abi_str)
        contract = w3.eth.contract(abi=abi)
        topics = topics_str.split(',')

        event_abi_list = [i for i in abi if 'name' in i and topics[0] == encode_hex(event_abi_to_log_topic(i))]

        if len(event_abi_list) == 0:
            raise Exception('Could not find any event with matching selector.')

        schema = event_abi_list[0].get('inputs')
        name = event_abi_list[0].get('name')

        event = contract.events[name]().processLog({
            'data': data,
            'topics': [decode_hex(topic) for topic in topics],
            # Placeholder
            'logIndex': '',
            'transactionIndex': '',
            'transactionHash': '',
            'address': '',
            'blockHash': '',
            'blockNumber': ''
        })

        return {name: _flatten_params(params=event.args, schema=schema)}
    except Exception as ex:
        print(f'parsing log data with abi failed: {str(ex)}')
        return None
