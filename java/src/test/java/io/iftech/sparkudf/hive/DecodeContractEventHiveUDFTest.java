package io.iftech.sparkudf.hive;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.iftech.sparkudf.converter.Converter;
import java.math.BigDecimal;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class DecodeContractEventHiveUDFTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testDecodeContractEvent() throws HiveException {
        ContractEvent e = new ContractEvent("Transfer");
        e.inputs = ImmutableList.of(
            new Field("from", "address", true),
            new Field("to", "address", true),
            new Field("value", "uint256"));

        DecodeContractEventHiveImpl impl = new DecodeContractEventHiveImpl();

        // Mock input object inspector
        BinaryObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        ListObjectInspector valueOI2 = ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] arguments = {valueOI1, valueOI2, valueOI3, valueOI4};
        StandardStructObjectInspector resultOI = (StandardStructObjectInspector) impl.initialize(
            arguments);

        // Mock input data
        DeferredObject valueDf1 = new DeferredJavaObject(
            Converter.decodeHexStartsWith0x(
                "0x000000000000000000000000000000000000000000003b23f6365b3fabec0000")
        );

        DeferredObject valueDf2 = new DeferredJavaObject(
            ImmutableList.of(
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000b3f923eabaf178fc1bd8e13902fc5c61d3ddef5b",
                "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60"
            )
        );

        DeferredObject valueDf3 = new DeferredJavaObject(gson.toJson(e));
        DeferredObject valueDf4 = new DeferredJavaObject("Transfer");
        DeferredObject[] args = {valueDf1, valueDf2, valueDf3, valueDf4};
        List<Object> result = (List<Object>) impl.evaluate(args);

        // Parse output data by output inspector and check it
        StandardStructObjectInspector inputOI = (StandardStructObjectInspector) resultOI.getStructFieldRef(
            "input").getFieldObjectInspector();
        List<Object> inputData = (List<Object>) resultOI.getStructFieldData(result,
            resultOI.getStructFieldRef("input"));
        List<Object> resultData = inputOI.getStructFieldsDataAsList(inputData);

        assertEquals("0xb3f923eabaf178fc1bd8e13902fc5c61d3ddef5b", resultData.get(0).toString());
        assertEquals("0x28c6c06298d514db089934071355e5743bf21d60", resultData.get(1).toString());
        assertEquals(0, ((HiveDecimalWritable) resultData.get(2))
            .getHiveDecimal().bigDecimalValue()
            .compareTo(new BigDecimal("279283000000000000000000")));
    }

    protected static class Field {

        String name;
        String type;
        boolean indexed;

        List<DecodeContractEventHiveUDFTest.Field> components;

        Field(String name, String type, boolean indexed) {
            this.name = name;
            this.type = type;
            this.indexed = indexed;
        }

        Field(String name, String type) {
            this(name, type, false);
        }
    }

    protected static class ContractEvent {

        String name = "test_event";
        String type = "event";
        List<DecodeContractEventHiveUDFTest.Field> inputs;

        ContractEvent(String name) {
            this.name = name;
        }
    }
}
