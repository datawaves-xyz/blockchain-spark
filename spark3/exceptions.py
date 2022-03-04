
from pyspark.sql import DataFrame


class TypeNotSupported(Exception):
    """
    We failed to decode ABI type to Spark type
    """

    def __init__(self, type_str: str) -> None:
        message = "Solidity type not supported: %s" % type_str
        super().__init__(message)


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

    def __init__(self) -> None:
        message = "Please set ABI json before call any method of contract"
        super().__init__(message)


class FunctionOrEventNotInContractABI(Exception):
    """
    We failed to find function or event
    """

    def __init__(self) -> None:
        pass
