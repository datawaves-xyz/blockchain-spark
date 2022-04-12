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
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            // test_bigint
            PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
            // test_address
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            // test_bytes
            PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
            // test_bool
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
            // test_M_bytes
            PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
            // test_int_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaIntObjectInspector),
            // test_address_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector),
            // test_bigint_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector),
            // test_bytes_array
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector),
            // data
            ObjectInspectorFactory.getStandardStructObjectInspector(
                ImmutableList.of("test_inner_tuple"),
                ImmutableList.of(
                    ObjectInspectorFactory.getStandardStructObjectInspector(
                        ImmutableList.of("f3.inner"),
                        ImmutableList.of(
                            PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector)
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
