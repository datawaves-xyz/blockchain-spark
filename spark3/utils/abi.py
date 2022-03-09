import json

from typing import cast, List

from spark3.types import ABI, ABIElement


def normalize_abi(abi: str) -> ABI:
    """Convert a json ABI string to an :class:`ABI` object which uses TypedDict"""
    abi = json.loads(abi)
    return cast(ABI, abi)


def filter_by_type(_type: str, contract_abi: ABI) -> List[ABIElement]:
    return [abi for abi in contract_abi if abi['type'] == _type]


def filter_by_name(name: str, contract_abi: ABI) -> List[ABIElement]:
    return [
        abi
        for abi
        in contract_abi
        if (
                abi['type'] not in ('fallback', 'constructor', 'receive')
                and abi['name'] == name
        )
    ]
