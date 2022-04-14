package io.iftech.sparkudf.spark;

import io.iftech.sparkudf.Decoder;
import io.iftech.sparkudf.converter.Converter;
import io.iftech.sparkudf.converter.SparkConverter;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;

public class DecodeContractFunctionUDF implements UDF4<byte[], byte[], String, String, Row> {

    @Override
    public Row call(
        final byte[] inputData,
        final byte[] outputData,
        final String functionABI,
        final String functionName
    ) throws Exception {
        List<Object> values = new Decoder<>(new SparkConverter())
            .decodeFunction(inputData, outputData, functionABI, functionName);

        return Row.fromSeq(Converter.convertListToSeq(values));
    }
}
