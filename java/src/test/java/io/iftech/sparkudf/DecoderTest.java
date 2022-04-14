package io.iftech.sparkudf;

import static org.junit.Assert.assertEquals;

import com.esaulpaugh.headlong.abi.Function;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.iftech.sparkudf.Mocks.ContractFunction;
import io.iftech.sparkudf.Mocks.FunctionField;
import org.junit.Test;

public class DecoderTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testNullNameInputAndOutput() throws Exception {
        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(new FunctionField("uint256"), new FunctionField("bytes"));
        f.outputs = ImmutableList.of(new FunctionField("uint256"), new FunctionField("bytes"));

        Function function = Function.fromJson(gson.toJson(f));
        Decoder.reviseElementName(function.getInputs(), "");
        Decoder.reviseElementName(function.getOutputs(), "output");

        assertEquals("_0", function.getInputs().get(0).getName());
        assertEquals("_1", function.getInputs().get(1).getName());
        assertEquals("output_0", function.getOutputs().get(0).getName());
        assertEquals("output_1", function.getOutputs().get(1).getName());
    }
}