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
import io.iftech.sparkudf.Mocks.ContractFunction;
import io.iftech.sparkudf.Mocks.FunctionField;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class DecodeContractFunctionHiveUDFTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testDecodeContractFunction() throws HiveException {
        ContractFunction f = new ContractFunction();
        f.inputs = ImmutableList.of(
            new FunctionField("test_string", "string"),
            new FunctionField("test_bigint", "uint256"),
            new FunctionField("test_address", "address"),
            new FunctionField("test_bytes", "bytes"),
            new FunctionField("test_bool", "bool"),
            new FunctionField("test_M_bytes", "bytes2"),
            new FunctionField("test_int_array", "int8[2]"),
            new FunctionField("test_address_array", "address[1]"),
            new FunctionField("test_bigint_array", "uint256[1]"),
            new FunctionField("test_bytes_array", "bytes4[1]"),
            new FunctionField("data", "tuple", ImmutableList.of(
                new FunctionField("test_inner_tuple", "tuple", ImmutableList.of(
                    new FunctionField("f3.inner", "uint256")))))
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
        assertEquals(new Text("100000000000"), resultData.get(1));
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b", resultData.get(2).toString());
        assertArrayEquals(new byte[]{0, 1, 1, 0}, ((BytesWritable) resultData.get(3)).getBytes());
        assertTrue(((BooleanWritable) resultData.get(4)).get());
        assertArrayEquals(new byte[]{9, 9}, ((BytesWritable) resultData.get(5)).getBytes());

        assertEquals(2, ((List<Object>) resultData.get(6)).size());
        assertEquals(new Text("17"), ((List<Object>) resultData.get(6)).get(0));
        assertEquals(new Text("19"), ((List<Object>) resultData.get(6)).get(1));

        assertEquals(1, ((List<Object>) resultData.get(7)).size());
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b",
            ((List<Object>) resultData.get(7)).get(0).toString());

        assertEquals(1, ((List<Object>) resultData.get(8)).size());
        assertEquals(new Text("400000000000"), ((List<Object>) resultData.get(8)).get(0));

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

        assertEquals(new Text("300000000000"), testInnerTupleObjects.get(0));
    }
}
