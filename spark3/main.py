from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, col, expr
from pyspark.sql.types import StructType

from spark3.ethereum import Contract
from spark3.providers import ContractABIProvider, EtherscanABIProvider
from spark3.exceptions import ColumnNotFoundInDataFrame


class Spark3:
    EtherscanABIProvider = EtherscanABIProvider

    def __init__(self, spark: SparkSession, provider: ContractABIProvider = None):
        self.contractABIProvider = provider
        self.trace_df = spark.sql("select * from ethereum.traces")
        self.log_df = spark.sql("select * from ethereum.logs_optimize")
        self._transformer = Transformer(spark)

    def contract(self, address: str):
        return Contract(address, self)

    def transformer(self):
        return self._transformer


DECODE_CONTRACT_FUNCTION_UDF = "io.iftech.sparkudf.DecodeContractFunctionUDF"
DECODE_CONTRACT_EVENT_UDF = "io.iftech.sparkudf.DecodeContractEventUDF"


class Transformer:
    """Wraps the transform logic which are implemented using external Java UDF"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def parse_trace_to_function(self, df: DataFrame,
                                abi: str,
                                schema: StructType,
                                name: str) -> DataFrame:

        self.spark.udf.registerJavaFunction("decode_func_%s" % name, DECODE_CONTRACT_FUNCTION_UDF, schema)

        if len([col_name for col_name, col_type in df.dtypes if
                col_name == "input" and col_type == "string"]) != 1:
            raise ColumnNotFoundInDataFrame("input", df)

        return df \
            .withColumn("abi", lit(abi)) \
            .withColumn("func_name", lit(name)) \
            .withColumn("function_parameter", expr("decode_func_%s(input, abi, func_name)" % name)) \
            .drop("abi") \
            .drop("input") \
            .drop("func_name") \
            .filter(col('function_parameter').isNotNull())

    def parse_log_to_event(self, df: DataFrame,
                           abi: str,
                           schema: StructType,
                           name: str) -> DataFrame:

        self.spark.udf.registerJavaFunction("decode_evt_%s" % name, DECODE_CONTRACT_EVENT_UDF, schema)

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
            .drop("abi") \
            .drop("data") \
            .drop("evt_name") \
            .filter(col('event_parameter').isNotNull())
