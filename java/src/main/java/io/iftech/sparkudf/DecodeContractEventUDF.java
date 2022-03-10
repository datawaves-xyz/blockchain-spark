package io.iftech.sparkudf;

import com.esaulpaugh.headlong.abi.Event;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

public class DecodeContractEventUDF implements
    UDF4<byte[], WrappedArray<String>, String, String, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractEventUDF.class);

    @Override
    public Row call(byte[] data, WrappedArray<String> topicsHex, String eventABI, String eventName)
        throws Exception {

        Event e = Event.fromJson(eventABI);
        if (!e.getName().equals(eventName)) {
            throw new IllegalArgumentException("Event name not match, eventABI=" + eventABI);
        }
        // prevent bad data
        if (topicsHex == null) {
            return null;
        }

        Tuple tuple;
        try {
            byte[][] topics = JavaConverters.seqAsJavaList(topicsHex).stream()
                .map(ContractDecoder::decodeHexStartsWith0x).toArray(byte[][]::new);
            tuple = e.decodeArgs(topics, data);
        } catch (Exception ex) {
            LOG.info("Fail to decode event, name=" + e.getName(), ex);
            return null;
        }
        TupleType tupleType = e.getInputs();

        return Row.fromSeq(ContractDecoder.convertListToSeq(ImmutableList.of(
            ContractDecoder.buildRowFromTuple(tupleType, tuple))));
    }
}
