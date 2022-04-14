package io.iftech.sparkudf.hive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class DecodeContractFunctionHiveUDFTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testDecodeContractFunction() throws HiveException {
        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new Field("test_string", "string"),
            new Field("test_bigint", "uint256"),
            new Field("test_address", "address"),
            new Field("test_bytes", "bytes"),
            new Field("test_bool", "bool"),
            new Field("test_M_bytes", "bytes2"),
            new Field("test_int_array", "int8[2]"),
            new Field("test_address_array", "address[1]"),
            new Field("test_bigint_array", "uint256[1]"),
            new Field("test_bytes_array", "bytes4[1]"),
            new Field("data", "tuple", ImmutableList.of(
                new Field("test_inner_tuple", "tuple", ImmutableList.of(
                    new Field("f3.inner", "uint256")))))
        );

        Function function = Function.fromJson(gson.toJson(f));
        ByteBuffer bytes = function.encodeCall(
            Tuple.of(
                "This is a string",
                BigInteger.valueOf(100000000000L),
                Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"),
                new byte[]{0, 1, 1, 0},
                true,
                new byte[]{9, 9},
                new int[]{17, 19},
                new Address[]{Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b")},
                new BigInteger[]{BigInteger.valueOf(400000000000L)},
                new byte[][]{new byte[]{0, 1, 1, 0}},
                Tuple.of(
                    Tuple.of(BigInteger.valueOf(300000000000L)))
            )
        );

        DecodeContractFunctionHiveUDF impl = new DecodeContractFunctionHiveImpl();

        // Mock input object inspector
        BinaryObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        BinaryObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] arguments = {valueOI1, valueOI2, valueOI3, valueOI4};
        StandardStructObjectInspector resultOI = (StandardStructObjectInspector) impl.initialize(
            arguments);

        // Mock input data
        DeferredObject valueDf1 = new DeferredJavaObject(bytes.array());
        DeferredObject valueDf2 = new DeferredJavaObject(new byte[]{});
        DeferredObject valueDf3 = new DeferredJavaObject(gson.toJson(f));
        DeferredObject valueDf4 = new DeferredJavaObject("test_function");
        DeferredObject[] args = {valueDf1, valueDf2, valueDf3, valueDf4};
        List<Object> result = (List<Object>) impl.evaluate(args);

        // Parse output data by output inspector and check it
        StandardStructObjectInspector inputOI = (StandardStructObjectInspector) resultOI.getStructFieldRef(
            "input").getFieldObjectInspector();
        List<Object> inputData = (List<Object>) resultOI.getStructFieldData(result,
            resultOI.getStructFieldRef("input"));
        List<Object> resultData = inputOI.getStructFieldsDataAsList(inputData);

        assertEquals("This is a string", resultData.get(0).toString());
        assertEquals(0, ((HiveDecimalWritable) resultData.get(1)).getHiveDecimal().bigDecimalValue()
            .compareTo(new BigDecimal("100000000000")));
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b", resultData.get(2).toString());
        assertArrayEquals(new byte[]{0, 1, 1, 0}, ((BytesWritable) resultData.get(3)).getBytes());
        assertTrue(((BooleanWritable) resultData.get(4)).get());
        assertArrayEquals(new byte[]{9, 9}, ((BytesWritable) resultData.get(5)).getBytes());

        assertEquals(2, ((List<Object>) resultData.get(6)).size());
        assertEquals(17, ((IntWritable) ((List<Object>) resultData.get(6)).get(0)).get());
        assertEquals(19, ((IntWritable) ((List<Object>) resultData.get(6)).get(1)).get());

        assertEquals(1, ((List<Object>) resultData.get(7)).size());
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",
            ((List<Object>) resultData.get(7)).get(0).toString());

        assertEquals(1, ((List<Object>) resultData.get(8)).size());
        assertEquals(0,
            ((HiveDecimalWritable) ((List<Object>) resultData.get(8)).get(
                0)).getHiveDecimal().bigDecimalValue().compareTo(new BigDecimal("400000000000")));

        assertEquals(1, ((List<Object>) resultData.get(9)).size());
        assertArrayEquals(new byte[]{0, 1, 1, 0},
            ((BytesWritable) ((List<Object>) resultData.get(9)).get(0)).getBytes());

        StandardStructObjectInspector dataOI = (StandardStructObjectInspector) inputOI.getStructFieldRef(
            "data").getFieldObjectInspector();
        List<Object> dataObjects = dataOI.getStructFieldsDataAsList(resultData.get(10));

        StandardStructObjectInspector testInnerTupleOI = (StandardStructObjectInspector) dataOI.getStructFieldRef(
            "test_inner_tuple").getFieldObjectInspector();
        List<Object> testInnerTupleObjects = testInnerTupleOI.getStructFieldsDataAsList(
            dataObjects.get(0));

        assertEquals(0,
            ((HiveDecimalWritable) testInnerTupleObjects.get(0)).getHiveDecimal().bigDecimalValue()
                .compareTo(new BigDecimal("300000000000")));
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
