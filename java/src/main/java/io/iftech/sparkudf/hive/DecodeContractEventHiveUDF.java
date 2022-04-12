package io.iftech.sparkudf.hive;

import com.google.crypto.tink.subtle.Hex;
import io.iftech.sparkudf.Decoder;
import io.iftech.sparkudf.converter.HiveConverter;
import io.iftech.sparkudf.spark.DecodeContractEventUDF;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
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


public abstract class DecodeContractEventHiveUDF extends GenericUDF {

    private static final Logger LOG = LoggerFactory.getLogger(DecodeContractEventUDF.class);

    private BinaryObjectInspector dataOI;
    private ListObjectInspector topicsHexOI;
    private StringObjectInspector eventABIOI;
    private StringObjectInspector eventNameOI;

    public abstract List<String> getInputDataFieldsName();

    public abstract List<ObjectInspector> getInputDataFieldsOIs();

    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
        throws UDFArgumentException {
        assert (args.length == 4);

        assert (args[0].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[0]).getPrimitiveCategory()
            == PrimitiveCategory.BINARY);
        dataOI = (BinaryObjectInspector) args[0];

        assert (args[1].getCategory() == Category.LIST);
        topicsHexOI = (ListObjectInspector) args[1];

        assert (args[2].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[2]).getPrimitiveCategory()
            == PrimitiveCategory.STRING);
        eventABIOI = (StringObjectInspector) args[2];

        assert (args[3].getCategory() == Category.PRIMITIVE);
        assert (((PrimitiveObjectInspector) args[3]).getPrimitiveCategory()
            == PrimitiveCategory.STRING);
        eventNameOI = (StringObjectInspector) args[3];

        StandardStructObjectInspector inputOI = ObjectInspectorFactory.getStandardStructObjectInspector(
            getInputDataFieldsName(), getInputDataFieldsOIs());

        return ObjectInspectorFactory.getStandardStructObjectInspector(
            ImmutableList.of("input"), ImmutableList.of(inputOI));
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args.length != 4) {
            return null;
        }

        byte[] data = dataOI.getPrimitiveJavaObject(args[0].get());
        List<String> topicsHex = (List<String>) topicsHexOI.getList(args[1].get());
        String eventABI = eventABIOI.getPrimitiveJavaObject(args[2].get());
        String eventName = eventNameOI.getPrimitiveJavaObject(args[3].get());

        List<Object> values;

        try {
            values = new Decoder<>(new HiveConverter())
                .decodeEvent(data, topicsHex, eventABI, eventName);
        } catch (Exception ex) {
            LOG.error(
                "Decode event meeting problems, data: " + Hex.encode(data)
                    + ", topicsHex: " + String.join(",", topicsHex) +
                    ", decodeTopics: " + String.join(",", topicsHex) +
                    ", eventABI:" + eventABI +
                    ", eventName: " + eventName, ex
            );
            return null;
        }

        return values;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return getStandardDisplayString("DecodeContractEventHiveUDF", strings);
    }
}
