from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, expr, unhex
from pyspark.sql.types import StructType

from spark3.ethereum.condition import Conditions
from spark3.ethereum.contract import Contract
from spark3.exceptions import ColumnNotFoundInDataFrame
from spark3.providers import IContractABIProvider, EtherscanABIProvider
from spark3.utils.df_util import contains_column


class Spark3:
    EtherscanABIProvider = EtherscanABIProvider

    def __init__(self, spark: SparkSession,
                 trace: Optional[DataFrame] = None,
                 log: Optional[DataFrame] = None,
                 trace_conditions: Optional[Conditions] = None,
                 log_conditions: Optional[Conditions] = None):
        """
        :param spark: :class:`SparkSession`
        :param trace: :class:`DataFrame` to store original data to decode contract functions
        :param log: :class:`DataFrame` to store original data to decode contract events
        :param trace_conditions: :class: `Conditions` to define the data selectors for trace table
        :param log_conditions: :class: `Conditions` to define the log selectors for log table
        """

        self.spark = spark
        self.trace_df = trace
        self.log_df = log
        self.trace_conditions = trace_conditions
        self.log_conditions = log_conditions
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

        if not contains_column(df.dtypes, "input", "string"):
            raise ColumnNotFoundInDataFrame("input(string)", df)

        if not contains_column(df.dtypes, "unhex_input", "binary"):
            # len is required argument in substring, use expr(substring) to replace
            df = df.withColumn("unhex_input", unhex(Transformer._provides_unhex_expr("input")))

        if contains_column(df.dtypes, "output", "string"):
            if not contains_column(df.dtypes, "unhex_output", "binary"):
                # len is required argument in substring, use expr(substring) to replace
                df = df.withColumn("unhex_output", unhex(Transformer._provides_unhex_expr("output")))

        return df \
            .withColumn("abi", lit(abi)) \
            .withColumn("func_name", lit(name)) \
            .withColumn("function_parameter", expr("decode_func_%s(unhex_input, unhex_output, abi, func_name)" % name)) \
            .filter(col('function_parameter').isNotNull()) \
            .drop("abi") \
            .drop("input") \
            .drop("output") \
            .drop("func_name")

    @staticmethod
    def parse_log_to_event(df: DataFrame,
                           abi: str,
                           schema: StructType,
                           name: str) -> DataFrame:

        df.sql_ctx.udf.registerJavaFunction("decode_evt_%s" % name, DECODE_CONTRACT_EVENT_UDF, schema)

        if not contains_column(df.dtypes, "data", "string"):
            raise ColumnNotFoundInDataFrame("data(string)", df)

        if not contains_column(df.dtypes, "unhex_data", "binary"):
            # len is required argument in substring, use expr(substring) to replace
            df = df.withColumn("unhex_data", unhex(Transformer._provides_unhex_expr("data")))

        if not contains_column(df.dtypes, "topics_arr", "array<string>"):
            raise ColumnNotFoundInDataFrame("topics_arr(array<string>)", df)

        return df \
            .withColumn("abi", lit(abi)) \
            .withColumn("evt_name", lit(name)) \
            .withColumn("event_parameter", expr("decode_evt_%s(unhex_data, topics_arr, abi, evt_name)" % name)) \
            .filter(col('event_parameter').isNotNull()) \
            .drop("abi") \
            .drop("data") \
            .drop("evt_name")

    @staticmethod
    def _provides_unhex_expr(col_name: str) -> expr:
        return expr(f'IF(instr({col_name}, "0x")=1, substring(input, 3), input)')
