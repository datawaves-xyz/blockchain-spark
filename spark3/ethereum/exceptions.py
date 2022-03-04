class TypeNotSupported(Exception):
    """
    We failed to decode ABI type to Spark type
    """

    def __init__(self, type_str: str) -> None:
        message = "Solidity type not supported: %s" % type_str
        super().__init__(message)


class ABITypeNotValid(Exception):
    """
    We failed to create ABI type
    """

    def __init__(self, message) -> None:
        super().__init__(message)
