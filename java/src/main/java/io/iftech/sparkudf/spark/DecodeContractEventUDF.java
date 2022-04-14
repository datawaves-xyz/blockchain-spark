package io.iftech.sparkudf.spark;

import io.iftech.sparkudf.Decoder;
import io.iftech.sparkudf.converter.Converter;
import io.iftech.sparkudf.converter.SparkConverter;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class DecodeContractEventUDF implements
    UDF4<byte[], WrappedArray<String>, String, String, Row> {

    @Override
    public Row call(
        final byte[] data,
        final WrappedArray<String> topicsHex,
        final String eventABI,
        final String eventName
    ) throws Exception {
        List<Object> values = new Decoder<>(new SparkConverter())
            .decodeEvent(
                data,
                JavaConverters.seqAsJavaList(topicsHex),
                eventABI,
                eventName
            );

        return Row.fromSeq(Converter.convertListToSeq(values));
    }
}
