from pyspark.sql import DataFrame


class FailToGetEtherscanABI(Exception):
    """
    We failed to get ABI from Etherscan
    """
    pass


class ColumnNotFoundInDataFrame(Exception):
    """
    We failed to find columns in a Dataframe
    """

    def __init__(self, name: str, df: DataFrame) -> None:
        message = "Column %s not found in DataFrame: %s" % (name, df.schema.simpleString())
        super().__init__(message)


class ContractABINotConfigured(Exception):
    """
    We failed to find abi json in contract
    """

    def __init__(self, message) -> None:
        super().__init__(message)


class ABIFunctionNotFound(Exception):
    """
    We failed to find function ABI
    """

    def __init__(self, message) -> None:
        super().__init__(message)


class ABIEventNotFound(Exception):
    """
    We failed to find event ABI
    """

    def __init__(self, message) -> None:
        super().__init__(message)

