package io.iftech.sparkudf.hive;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class OpenseaWyvernExchangeV2AtomicMatch extends DecodeContractFunctionHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of("addrs", "uints", "feeMethodsSidesKindsHowToCalls", "calldataBuy",
            "calldataSell", "replacementPatternBuy", "replacementPatternSell", "staticExtradataBuy",
            "staticExtradataSell", "vs", "rssMetadata");
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of(ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector),
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector),
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector));
    }

    @Override
    public List<String> getOutputDataFieldsName() {
        return ImmutableList.of();
    }

    @Override
    public List<ObjectInspector> getOutputDataFieldsOIs() {
        return ImmutableList.of();
    }
}
