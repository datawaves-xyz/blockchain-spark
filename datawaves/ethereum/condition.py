from eth_utils import encode_hex, event_abi_to_log_topic, function_abi_to_4byte_selector

from datawaves.utils import hash_unsafe_bytes
from spark3.ethereum.condition import Condition, Conditions


def _return_self_condition(alias: str, _type: str) -> Condition[str, str]:
    return Condition(key_alias=alias, key_type=_type, transform=lambda x: x)


def _return_hash_condition(alias: str, _type: str) -> Condition[str, int]:
    return Condition(key_alias=alias, key_type=_type, transform=lambda x: abs(hash_unsafe_bytes(x)) % 10)


def new_trace_conditions():
    """
    Only for `ethereum.traces` table in Datawaves platform
    """
    default_trace_function_selector_condition = Condition(
        key_alias='selector',
        key_type='string',
        # :paras x: function abi dict
        transform=lambda x: encode_hex(function_abi_to_4byte_selector(x))
    )

    default_trace_function_hash_condition = Condition(
        key_alias='selector_hash',
        key_type='int',
        # :paras x: function abi dict
        transform=lambda x: abs(hash_unsafe_bytes(encode_hex(function_abi_to_4byte_selector(x)))) % 10
    )

    return Conditions(
        address_condition=_return_self_condition('to_address', 'string'),
        address_hash_condition=_return_hash_condition('address_hash', 'int'),
        selector_condition=default_trace_function_selector_condition,
        selector_hash_condition=default_trace_function_hash_condition
    )


def new_log_conditions():
    """
    Only for `ethereum.logs` table in Datawaves platform
    """
    default_log_event_selector_condition = Condition(
        key_alias='selector',
        key_type='string',
        # :paras x: event abi dict
        transform=lambda x: encode_hex(event_abi_to_log_topic(x))
    )

    default_log_event_hash_condition = Condition(
        key_alias='selector_hash',
        key_type='int',
        # :paras x: event abi dict
        transform=lambda x: abs(hash_unsafe_bytes(encode_hex(event_abi_to_log_topic(x)))) % 10
    )

    return Conditions(
        address_condition=_return_self_condition('address', 'string'),
        address_hash_condition=_return_hash_condition('address_hash', 'int'),
        selector_condition=default_log_event_selector_condition,
        selector_hash_condition=default_log_event_hash_condition
    )
