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

        Tuple inputTuple, outputTuple;
        Row inputResult = null;
        Row outputResult = null;

        try {
            byte[] inputData = ContractDecoder.decodeHexStartsWith0x(inputHex);
            byte[] outputData = ContractDecoder.decodeHexStartsWith0x(outputHex);

            // See why BytesBuffer is useful: https://github.com/esaulpaugh/headlong/issues/34
            inputTuple = f.decodeCall(ByteBuffer.wrap(inputData));
            outputTuple = f.decodeReturn(ByteBuffer.wrap(outputData));

        } catch (IllegalArgumentException ex) {
            // silent about the wrong selector error
            return null;
        } catch (Exception ex) {
            LOG.info("Fail to decode function, name=" + f.getName(), ex);
            return null;
        }

        inputResult = ContractDecoder.buildRowFromTuple(f.getInputs(), inputTuple);
        outputResult = ContractDecoder.buildRowFromTuple(f.getOutputs(), outputTuple);

        List<Object> values = new LinkedList<>();
        if (inputResult.length() > 0) {
            values.add(inputResult);
        }
        if (outputResult.length() > 0) {
            values.add(outputResult);
        }
        return Row.fromSeq(ContractDecoder.convertListToSeq(values));
    }
}
