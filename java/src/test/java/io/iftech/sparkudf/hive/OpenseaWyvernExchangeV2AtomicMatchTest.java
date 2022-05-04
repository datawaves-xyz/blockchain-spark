package io.iftech.sparkudf.hive;

import static io.iftech.sparkudf.TestUtils.hexStringToByteArray;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.iftech.sparkudf.Mocks.ContractFunction;
import io.iftech.sparkudf.Mocks.FunctionField;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class OpenseaWyvernExchangeV2AtomicMatchTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testDecodeContractFunction() throws HiveException {
        ContractFunction f = new ContractFunction("atomicMatch_");
        f.inputs = ImmutableList.of(
            new FunctionField("addrs", "address[14]"),
            new FunctionField("uints", "uint256[18]"),
            new FunctionField("feeMethodsSidesKindsHowToCalls", "uint8[8]"),
            new FunctionField("calldataBuy", "bytes"),
            new FunctionField("calldataSell", "bytes"),
            new FunctionField("replacementPatternBuy", "bytes"),
            new FunctionField("replacementPatternSell", "bytes"),
            new FunctionField("staticExtradataBuy", "bytes"),
            new FunctionField("staticExtradataSell", "bytes"),
            new FunctionField("vs", "uint8[2]"),
            new FunctionField("rssMetadata", "bytes32[5]")
        );

        byte[] bytes = hexStringToByteArray(
            "ab834bab0000000000000000000000007f268357a8c2552623316e2562d90e642bb538e500000000000000000000000066c7f46b0cb2482119c33010f89ebfd3591f936f0000000000000000000000009075da9cb946e393bcc8e651000a3935d691fa130000000000000000000000000000000000000000000000000000000000000000000000000000000000000000baf2127b49fc93cbca6269fade0f7f31df4c88a7000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000007f268357a8c2552623316e2562d90e642bb538e50000000000000000000000009075da9cb946e393bcc8e651000a3935d691fa1300000000000000000000000000000000000000000000000000000000000000000000000000000000000000005b3256965e7c3cf26e11fcaf296dfc8807c01073000000000000000000000000baf2127b49fc93cbca6269fade0f7f31df4c88a70000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002ee00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000069789fbbc4f80000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000626e6caa00000000000000000000000000000000000000000000000000000000000000001ac7a1f97da3450ba68ad5e6c1efd26c70e412738e353468dadb15eb1238bbcd00000000000000000000000000000000000000000000000000000000000002ee00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000069789fbbc4f80000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000626e6c250000000000000000000000000000000000000000000000000000000062974ae39c909c767a5c939879b43a54860853360793b4116c1b1d7f606107588abeb0da0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000006a000000000000000000000000000000000000000000000000000000000000007c000000000000000000000000000000000000000000000000000000000000008e00000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000b200000000000000000000000000000000000000000000000000000000000000b20000000000000000000000000000000000000000000000000000000000000001b000000000000000000000000000000000000000000000000000000000000001b866c2865df9c67340f22d373e9119562fcfbb11d705519321d74f4f9e10182112320dcfcc97001677f176e7df04f47b7bff78ef8d000444fe7ef0e1e695e700e866c2865df9c67340f22d373e9119562fcfbb11d705519321d74f4f9e10182112320dcfcc97001677f176e7df04f47b7bff78ef8d000444fe7ef0e1e695e700e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e4fb16a595000000000000000000000000000000000000000000000000000000000000000000000000000000000000000066c7f46b0cb2482119c33010f89ebfd3591f936f00000000000000000000000034d85c9cdeb23fa97cb08333b511ac86e1c4e258000000000000000000000000000000000000000000000000000000000000b0b6000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e4fb16a5950000000000000000000000009075da9cb946e393bcc8e651000a3935d691fa13000000000000000000000000000000000000000000000000000000000000000000000000000000000000000034d85c9cdeb23fa97cb08333b511ac86e1c4e258000000000000000000000000000000000000000000000000000000000000b0b6000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e400000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000e4000000000000000000000000000000000000000000000000000000000000000000000000ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        OpenseaWyvernExchangeV2AtomicMatch impl = new OpenseaWyvernExchangeV2AtomicMatch();

        // Mock input object inspector
        BinaryObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        BinaryObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        ObjectInspector valueOI3 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        ObjectInspector valueOI4 = PrimitiveObjectInspectorFactory.javaStringObjectInspector;

        ObjectInspector[] arguments = {valueOI1, valueOI2, valueOI3, valueOI4};
        StandardStructObjectInspector resultOI = (StandardStructObjectInspector) impl.initialize(
            arguments);

        // Mock input data
        DeferredObject valueDf1 = new DeferredJavaObject(bytes);
        DeferredObject valueDf2 = new DeferredJavaObject(new byte[]{});
        DeferredObject valueDf3 = new DeferredJavaObject(gson.toJson(f));
        DeferredObject valueDf4 = new DeferredJavaObject("atomicMatch_");
        DeferredObject[] args = {valueDf1, valueDf2, valueDf3, valueDf4};
        List<Object> result = (List<Object>) impl.evaluate(args);

        // Parse output data by output inspector and check it
        StandardStructObjectInspector inputOI = (StandardStructObjectInspector) resultOI.getStructFieldRef(
            "input").getFieldObjectInspector();
        List<Object> inputData = (List<Object>) resultOI.getStructFieldData(result,
            resultOI.getStructFieldRef("input"));
        List<Object> resultData = inputOI.getStructFieldsDataAsList(inputData);

        List<Object> addrs = (List<Object>) resultData.get(0);
        assertEquals(14, addrs.size());
        assertEquals("0x7f268357a8c2552623316e2562d90e642bb538e5", addrs.get(0).toString());
        assertEquals("0x66c7f46b0cb2482119c33010f89ebfd3591f936f", addrs.get(1).toString());
        assertEquals("0x0000000000000000000000000000000000000000", addrs.get(13).toString());

        List<Object> uints = (List<Object>) resultData.get(1);
        assertEquals(18, uints.size());
        assertEquals(new Text("750"), uints.get(0));
        assertEquals(new Text("7600000000000000000"), uints.get(4));
        assertEquals(
            new Text(
                "12112854536475579701709856639006968439346510087985402025553784139104925563853"),
            uints.get(8));
        assertEquals(
            new Text(
                "70816310222907633346803407554718755161164435004871826408466484445498409136346"),
            uints.get(17));
    }
}
