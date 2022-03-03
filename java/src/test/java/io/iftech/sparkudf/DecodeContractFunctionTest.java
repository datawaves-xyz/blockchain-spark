package io.iftech.sparkudf;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.util.FastHex;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.sql.Row;
import org.junit.Test;

public class DecodeContractFunctionTest {

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
            new byte[] { 0, 1, 1, 0 },
            true,
            new byte[] { 9, 9 }));

    DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
    Row data = udf.call(FastHex.encodeToString(bytes.array()), gson.toJson(f), "test_function");
    assertEquals("This is a string", data.get(0));
    assertEquals(BigInteger.valueOf(100000000000L), data.get(1));
    assertEquals("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b", data.getString(2));
    assertArrayEquals(new byte[] { 0, 1, 1, 0 }, (byte[]) data.get(3));
    assertEquals(true, data.getBoolean(4));
    assertArrayEquals(new byte[] { 9, 9 }, (byte[]) data.get(5));
  }

  @Test
  public void testFixedSizeArrayInputsCanDecode() throws Exception {

    ContractFunction f = new ContractFunction();
    f.inputs = ImmutableList.of(
        new Field("data", "tuple", ImmutableList.of(
            new Field("test_int_array", "int8[2]"),
            new Field("test_address_array", "address[1]"),
            new Field("test_bigint_array", "uint256[1]"),
            new Field("test_bytes_array", "bytes4[1]"))));

    Function function = Function.fromJson(gson.toJson(f));
    ByteBuffer bytes = function.encodeCall(
        Tuple.of(
            Tuple.of(
                new int[] { 17, 19 },
                new Address[] { Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b") },
                new BigInteger[] { BigInteger.valueOf(400000000000L) },
                new byte[][] { new byte[] { 0, 1, 1, 0 } })));

    DecodeContractFunctionUDF udf = new DecodeContractFunctionUDF();
    Row row = udf.call(FastHex.encodeToString(bytes.array()), gson.toJson(f), "test_function");
    Row data = row.getStruct(0);
    assertArrayEquals(new int[] { 17, 19 }, (int[]) data.get(0));
    assertEquals(ImmutableList.of("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"), data.getList(1));
    assertArrayEquals(new BigInteger[] { BigInteger.valueOf(400000000000L) }, (BigInteger[]) data.get(2));
    assertArrayEquals(new byte[] { 0, 1, 1, 0 }, (byte[]) data.getList(3).get(0));
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
    Row row = udf.call(FastHex.encodeToString(bytes.array()), gson.toJson(f), "test_function");
    Row data = row.getStruct(0);
    assertEquals(BigInteger.valueOf(300000000000L), data.getStruct(0).get(0));
  }

  protected class Field {
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

  protected class ContractFunction {
    String name = "test_function";
    String type = "function";
    List<Field> inputs;
  }
}