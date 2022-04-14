package io.iftech.sparkudf.hive;

import com.google.crypto.tink.subtle.Hex;
import io.iftech.sparkudf.Decoder;
import io.iftech.sparkudf.converter.HiveConverter;
import io.iftech.sparkudf.spark.DecodeContractEventUDF;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.collect.ImmutableList;

public abstract class DecodeContractFunctionHiveUDF extends GenericUDF {

    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractEventUDF.class);

    private BinaryObjectInspector inputOI;
    private BinaryObjectInspector outputOI;
    private StringObjectInspector functionABIOI;
    private StringObjectInspector functionNameOI;

    public abstract List<String> getInputDataFieldsName();

    public abstract List<ObjectInspector> getInputDataFieldsOIs();

    public abstract List<String> getOutputDataFieldsName();

    public abstract List<ObjectInspector> getOutputDataFieldsOIs();

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
        throws UDFArgumentException {
        assert (args.length == 4);

        assert (args[0].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[0]).getPrimitiveCategory()
            == PrimitiveCategory.BINARY);
        inputOI = (BinaryObjectInspector) args[0];

        assert (args[1].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[1]).getPrimitiveCategory()
            == PrimitiveCategory.BINARY);
        outputOI = (BinaryObjectInspector) args[1];

        assert (args[2].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[2]).getPrimitiveCategory()
            == PrimitiveCategory.STRING);
        functionABIOI = (StringObjectInspector) args[2];

        assert (args[3].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[3]).getPrimitiveCategory()
            == PrimitiveCategory.STRING);
        functionNameOI = (StringObjectInspector) args[3];

        StandardStructObjectInspector inputOI = ObjectInspectorFactory.getStandardStructObjectInspector(
            getInputDataFieldsName(), getInputDataFieldsOIs());

        StandardStructObjectInspector outputOI = ObjectInspectorFactory.getStandardStructObjectInspector(
            getOutputDataFieldsName(), getOutputDataFieldsOIs());

        return ObjectInspectorFactory.getStandardStructObjectInspector(
            ImmutableList.of("input", "output"), ImmutableList.of(inputOI, outputOI));
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args.length != 4) {
            return null;
        }

        byte[] input = inputOI.getPrimitiveJavaObject(args[0].get());
        byte[] output = outputOI.getPrimitiveJavaObject(args[1].get());
        String functionABI = functionABIOI.getPrimitiveJavaObject(args[2].get());
        String functionName = functionNameOI.getPrimitiveJavaObject(args[3].get());

        List<Object> values;

        try {
            values = new Decoder<>(new HiveConverter())
                .decodeFunction(input, output, functionABI, functionName);
        } catch (Exception ex) {
            LOG.error(
                "Decode function meeting problems, input: " + Hex.encode(input) +
                    ", output: " + Hex.encode(output) +
                    ", functionABI:" + functionABI +
                    ", functionName: " + functionName, ex
            );
            return null;
        }

        return values;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
