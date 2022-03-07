from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.types import StructType

from spark3.ethereum import Contract
from spark3.providers import IContractABIProvider, EtherscanABIProvider
from spark3.exceptions import ColumnNotFoundInDataFrame


class Spark3:
    EtherscanABIProvider = EtherscanABIProvider

    def __init__(self, spark: SparkSession,
                 trace: DataFrame,
                 log: DataFrame):
        """
        :param spark: :class:`SparkSession`
        :param trace: :class:`DataFrame` to store original data to decode contract functions
        :param log: :class:`DataFrame` to store original data to decode contract events
        """
        self.spark = spark
        self.trace_df = trace
        self.log_df = log
        self._transformer = Transformer()

    def contract(self, address: str,
                 abi: Optional[str] = None,
                 abi_provider: Optional[IContractABIProvider] = None):
        """
        If abi is provided, then this method will return an instance of the contract defined by abi

        >>> from spark3 import Spark3
        >>>
        >>> s3 = Spark3(...)
        >>>
        >>>> contract = s3.contract(address=..., abi=...)

        If abi_provider is provided, then this method will return an instance of the contract defined by the
        provider and a given address

        >>> from spark3 import Spark3
        >>>
        >>> s3 = Spark3(...)
        >>>
        >>> contract = s3.contract(address=..., abi_provider=...)
        """
        return Contract(self, address, abi, abi_provider)

    def transformer(self):
        return self._transformer


DECODE_CONTRACT_FUNCTION_UDF = "io.iftech.sparkudf.DecodeContractFunctionUDF"
DECODE_CONTRACT_EVENT_UDF = "io.iftech.sparkudf.DecodeContractEventUDF"


class Transformer:
    """Wraps the transform logic which are implemented using external Java UDF"""

    @staticmethod
    def parse_trace_to_function(df: DataFrame,
                                abi: str,
                                schema: StructType,
                                name: str) -> DataFrame:

        df.sql_ctx.udf.registerJavaFunction("decode_func_%s" % name, DECODE_CONTRACT_FUNCTION_UDF, schema)

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "input" and col_type == "string"]) != 1:
            raise ColumnNotFoundInDataFrame("input", df)

        return df \
            .withColumn("abi", lit(abi)) \
            .withColumn("func_name", lit(name)) \
            .withColumn("function_parameter", expr("decode_func_%s(input, output, abi, func_name)" % name)) \
            .filter(col('function_parameter').isNotNull()) \
            .drop("abi") \
            .drop("input") \
            .drop("output") \
            .drop("func_name") \


    @staticmethod
    def parse_log_to_event(df: DataFrame,
                           abi: str,
                           schema: StructType,
                           name: str) -> DataFrame:

        df.sql_ctx.udf.registerJavaFunction("decode_evt_%s" % name, DECODE_CONTRACT_EVENT_UDF, schema)

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "data" and col_type == "string"]) != 1:
            raise ColumnNotFoundInDataFrame("data(string)", df)

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "topics_arr" and col_type == "array<string>"]) != 1:
            raise ColumnNotFoundInDataFrame("topics_arr(array<string>)", df)

        return df \
            .withColumn("abi", lit(abi)) \
            .withColumn("evt_name", lit(name)) \
            .withColumn("event_parameter", expr("decode_evt_%s(data, topics_arr, abi, evt_name)" % name)) \
            .filter(col('event_parameter').isNotNull()) \
            .drop("abi") \
            .drop("data") \
            .drop("evt_name")
