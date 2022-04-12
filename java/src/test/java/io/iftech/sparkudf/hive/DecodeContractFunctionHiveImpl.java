package io.iftech.sparkudf.hive;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class DecodeContractFunctionHiveImpl extends DecodeContractFunctionHiveUDF {

    @Override
    public List<String> getInputDataFieldsName() {
        return ImmutableList.of(
            "test_string", "test_bigint", "test_address", "test_bytes", "test_bool", "test_M_bytes",
            "test_int_array", "test_address_array", "test_bigint_array", "test_bytes_array", "data"
        );
    }

    @Override
    public List<ObjectInspector> getInputDataFieldsOIs() {
        return ImmutableList.of(
            // test_string
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            // test_bigint
            PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
            // test_address
            PrimitiveObjectInspectorFactory.writableStringObjectInspector,
            // test_bytes
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            // test_bool
            PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
            // test_M_bytes
            PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
            // test_int_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableIntObjectInspector),
            // test_address_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector),
            // test_bigint_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector),
            // test_bytes_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableBinaryObjectInspector),
            // data
            ObjectInspectorFactory.getStandardStructObjectInspector(
                ImmutableList.of("test_inner_tuple"),
                ImmutableList.of(
                    ObjectInspectorFactory.getStandardStructObjectInspector(
                        ImmutableList.of("f3.inner"),
                        ImmutableList.of(
                            PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector)
                    )
                )
            )
        );
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
