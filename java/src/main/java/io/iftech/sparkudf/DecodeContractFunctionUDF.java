package io.iftech.sparkudf;

import java.nio.ByteBuffer;

import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecodeContractFunctionUDF implements UDF3<String, String, String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractFunctionUDF.class);

    @Override
    public Row call(String inputHex, String functionABI, String functionName) throws Exception {

        Function f = Function.fromJson(functionABI);
        if (!f.getName().equals(functionName)) {
            throw new IllegalArgumentException("Function name not match, eventABI=" + functionABI);
        }

        Tuple tuple;
        try {
            byte[] inputData = ContractDecoder.decodeHexStartsWith0x(inputHex);

            // See: https://github.com/esaulpaugh/headlong/issues/34
            tuple = f.decodeCall(ByteBuffer.wrap(inputData));
        } catch (Exception ex) {
            LOG.info("Fail to decode function, name=" + f.getName() + ", input=" + inputHex
                    + ", errorMsg=" + ex.toString());
            return null;
        }
        TupleType tupleType = f.getInputs();
        return ContractDecoder.buildRowFromTuple(tupleType, tuple);
    }
}
