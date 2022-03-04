package io.iftech.sparkudf;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.LinkedList;

import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF4;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DecodeContractFunctionUDF implements UDF4<String, String, String, String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractFunctionUDF.class);

    @Override
    public Row call(String inputHex, String outputHex, String functionABI, String functionName) throws Exception {

        Function f = Function.fromJson(functionABI);
        if (!f.getName().equals(functionName)) {
            throw new IllegalArgumentException("Function name not match, eventABI=" + functionABI);
        }

        List<Object> values = new LinkedList<>();

        if (f.getInputs().size() > 0) {

            try {
                byte[] inputData = ContractDecoder.decodeHexStartsWith0x(inputHex);
                // See why BytesBuffer is useful:
                // https://github.com/esaulpaugh/headlong/issues/34
                Tuple inputTuple = f.decodeCall(ByteBuffer.wrap(inputData));
                Row inputResult = ContractDecoder.buildRowFromTuple(f.getInputs(), inputTuple);
                values.add(inputResult);
            } catch (Exception ex) {
                LOG.info("Fail to decode function, name=" + f.getName(), ex);
                return null;
            }
        }

        if (f.getOutputs().size() > 0) {
            try {
                byte[] outputData = ContractDecoder.decodeHexStartsWith0x(outputHex);
                Tuple outputTuple = f.decodeReturn(ByteBuffer.wrap(outputData));
                Row outputResult = ContractDecoder.buildRowFromTuple(f.getOutputs(), outputTuple);
                values.add(outputResult);
            } catch (Exception ex) {
                LOG.info("Fail to decode function, name=" + f.getName(), ex);
                return null;
            }
        }

        return Row.fromSeq(ContractDecoder.convertListToSeq(values));
    }
}
