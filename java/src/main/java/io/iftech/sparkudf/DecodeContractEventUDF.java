package io.iftech.sparkudf;

import java.util.List;

import com.esaulpaugh.headlong.abi.Event;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.JavaConversions;
import scala.collection.mutable.WrappedArray;

public class DecodeContractEventUDF implements UDF4<String, WrappedArray<String>, String, String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractEventUDF.class);

    @Override
    public Row call(String dataHex, WrappedArray<String> topicsHex, String eventABI, String eventName)
            throws Exception {

        Event e = Event.fromJson(eventABI);
        if (!e.getName().equals(eventName)) {
            throw new IllegalArgumentException("Event name not match, eventABI=" + eventABI);
        }

        Tuple tuple;
        try {
            byte[][] topics = JavaConversions.seqAsJavaList(topicsHex).stream()
                    .map(hex -> ContractDecoder.decodeHexStartsWith0x(hex)).toArray(byte[][]::new);
            byte[] data = ContractDecoder.decodeHexStartsWith0x(dataHex);
            tuple = e.decodeArgs(topics, data);
        } catch (Exception ex) {
            LOG.info("Fail to decode event, name=" + e.getName() + ", data=" + dataHex + ", topic=" +
                    topicsHex
                    + ", errorMsg=" + ex.toString());
            return null;
        }
        TupleType tupleType = e.getInputs();
        return ContractDecoder.buildRowFromTuple(tupleType, tuple);
    }
}
