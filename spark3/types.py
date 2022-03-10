from typing import (
    TypedDict,
    Sequence,
    Literal,
    Union
)


# event

class ABIEventElement(TypedDict, total=False):
    indexed: bool
    name: str
    type: str


class ABIEvent(TypedDict, total=False):
    anonymous: bool
    inputs: Sequence[ABIEventElement]
    name: str
    type: Literal["event"]


# function

class ABIFunctionComponents(TypedDict, total=False):
    components: Sequence['ABIFunctionElement']
    name: str
    type: str


class ABIFunctionElement(TypedDict, total=False):
    components: Sequence[ABIFunctionComponents]
    name: str
    type: str


class ABIFunction(TypedDict, total=False):
    constant: bool
    name: str
    inputs: Sequence[ABIFunctionElement]
    outputs: Sequence[ABIFunctionElement]
    payable: bool
    stateMutability: Literal["pure", "view", "nonpayable", "payable"]
    type: Literal["function", "constructor", "fallback", "receive"]


ABIElement = Union[ABIFunction, ABIEvent]

# Internal ABI type for safer function parameters
ABI = Sequence[ABIElement]
