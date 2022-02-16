import json

import pyspark.sql
from pyspark.sql.functions import udf, lit, col

from spark3.ethereum import decode_function_input_with_abi, get_call_schema, decode_log_data_with_abi


class Transformer:

    @staticmethod
    def parse_traces_input(df: pyspark.sql.DataFrame, abi: str) -> pyspark.sql.DataFrame:
        # The dataframe must container input column.
        assert (len([col_name for col_name, col_type in df.dtypes if
                     col_name == 'input' and col_type == 'string']) == 1)

        schema = get_call_schema(abi=json.loads(abi), mode='function')
        parse_traces_udf = udf(f=lambda input_data, abi_str: decode_function_input_with_abi(input_data, abi_str),
                               returnType=schema)

        return df.filter(col('input').isNotNull) \
            .withColumn('function_parameters', parse_traces_udf('input', lit(abi))) \
            .filter(col('function_parameters').isNotNull())

    @staticmethod
    def parse_logs_data(df: pyspark.sql.DataFrame, abi: str) -> pyspark.sql.DataFrame:
        # The dataframe must container topics and data column.
        assert (len([col_name for col_name, col_type in df.dtypes if
                     col_name == 'topics' and col_type == 'string']) == 1)

        assert (len([col_name for col_name, col_type in df.dtypes if
                     col_name == 'data' and col_type == 'string']) == 1)

        schema = get_call_schema(abi=json.loads(abi), mode='event')
        parse_logs_udf = udf(f=lambda data, topics, abi_str: decode_log_data_with_abi(data, topics, abi_str),
                             returnType=schema)

        return df.filter(col('data').isNotNull) \
            .withColumn('log_parameters', parse_logs_udf('data', 'topics', lit(abi))) \
            .filter(col('log_parameters').isNotNull())
