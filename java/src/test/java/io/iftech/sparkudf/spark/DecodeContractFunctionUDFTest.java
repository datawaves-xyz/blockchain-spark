package io.iftech.sparkudf.spark;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class DecodeContractFunctionUDFTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testElementaryInputsCanDecode() throws Exception {

        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new Field("test_string", "string"),
            new Field("test_bigint", "uint256"),
            new Field("test_address", "address"),
            new Field("test_bytes", "bytes"),
            new Field("test_bool", "bool"),
            new Field("test_M_bytes", "bytes2"));

        Function function = Function.fromJson(gson.toJson(f));
        ByteBuffer bytes = function.encodeCall(
            Tuple.of(
                "This is a string",
                BigInteger.valueOf(100000000000L),
                Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"),
                new byte[]{0, 1, 1, 0},
                true,
                new byte[]{9, 9}));

        DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
        Row result = udf.call(bytes.array(), new byte[]{}, gson.toJson(f), "test_function");
        Row input = result.getStruct(0);
        assertEquals("This is a string", input.get(0));
        assertEquals(BigInteger.valueOf(100000000000L), input.get(1));
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b", input.getString(2));
        assertArrayEquals(new byte[]{0, 1, 1, 0}, (byte[]) input.get(3));
        assertTrue(input.getBoolean(4));
        assertArrayEquals(new byte[]{9, 9}, (byte[]) input.get(5));
        assertEquals(1, result.length());
    }

    @Test
    public void testFixedSizeArrayInputsCanDecode() throws Exception {

        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new Field("test_int_array", "int8[2]"),
            new Field("test_address_array", "address[1]"),
            new Field("test_bigint_array", "uint256[1]"),
            new Field("test_bytes_array", "bytes4[1]"));

        Function function = Function.fromJson(gson.toJson(f));
        ByteBuffer bytes = function.encodeCall(
            Tuple.of(
                new int[]{17, 19},
                new Address[]{Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")},
                new BigInteger[]{BigInteger.valueOf(400000000000L)},
                new byte[][]{new byte[]{0, 1, 1, 0}}));

        DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
        Row result = udf.call(bytes.array(), new byte[]{}, gson.toJson(f),
            "test_function");
        Row input = result.getStruct(0);

        List<Integer> value1 = input.getList(0);
        assertEquals(2, value1.size());
        assertEquals(17, value1.get(0).intValue());
        assertEquals(19, value1.get(1).intValue());

        assertEquals(ImmutableList.of("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b"),
            input.getList(1));

        List<BigInteger> value3 = input.getList(2);
        assertEquals(1, value3.size());
        assertEquals(BigInteger.valueOf(400000000000L), value3.get(0));

        assertArrayEquals(new byte[]{0, 1, 1, 0}, (byte[]) input.getList(3).get(0));
        assertEquals(1, result.length());
    }

    @Test
    public void testNestedTupleInputsCanDecode() throws Exception {

        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new Field("data", "tuple", ImmutableList.of(
                new Field("test_inner_tuple", "tuple", ImmutableList.of(
                    new Field("f3.inner", "uint256"))))));

        Function function = Function.fromJson(gson.toJson(f));
        ByteBuffer bytes = function.encodeCall(
            Tuple.of(
                Tuple.of(
                    Tuple.of(BigInteger.valueOf(300000000000L)))));

        DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
        Row result = udf.call(bytes.array(), new byte[]{}, gson.toJson(f),
            "test_function");
        Row input = result.getStruct(0);
        assertEquals(BigInteger.valueOf(300000000000L), input.getStruct(0).getStruct(0).get(0));
        assertEquals(1, result.length());
    }

    @Test
    public void testOutputsCanBeNull() throws Exception {

        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new Field("one", "uint256"));

        Function function = Function.fromJson(gson.toJson(f));
        ByteBuffer bytes = function.encodeCall(
            Tuple.of(BigInteger.valueOf(300000000000L)));

        DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
        Row result = udf.call(bytes.array(), null, gson.toJson(f),
            "test_function");
        assertEquals(BigInteger.valueOf(300000000000L), result.getStruct(0).get(0));
        assertEquals(1, result.length());
    }

    protected static class Field {

        String name;
        String type;
        List<Field> components;

        Field(String name, String type) {
            this.name = name;
            this.type = type;
        }

        Field(String name, String type, List<Field> components) {
            this(name, type);
            this.components = components;
        }
    }

    protected static class ContractFunction {

        String name = "test_function";
        String type = "function";
        List<Field> inputs;
        List<Field> outputs;

    }
}