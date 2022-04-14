package io.iftech.sparkudf;

import com.esaulpaugh.headlong.abi.ABIType;
import com.esaulpaugh.headlong.abi.Event;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import io.iftech.sparkudf.converter.Converter;
import io.iftech.sparkudf.converter.SparkConverter;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.guava.base.Strings;
import org.sparkproject.guava.collect.ImmutableList;

public class Decoder<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Decoder.class);

    private final Converter<T> converter;

    public Decoder(Converter<T> converter) {
        this.converter = converter;
    }

    public List<Object> decodeEvent(
        final byte[] data,
        final List<String> topicsHex,
        final String eventABI,
        final String eventName
    ) {
        Event e = Event.fromJson(eventABI);
        reviseElementName(e.getInputs(), "");
        if (!e.getName().equals(eventName)) {
            throw new IllegalArgumentException("Event name not match, eventABI=" + eventABI);
        }

        // prevent bad data
        if (topicsHex == null) {
            return null;
        }

        Tuple tuple;
        try {
            byte[][] topics = topicsHex.stream()
                .map(SparkConverter::decodeHexStartsWith0x).toArray(byte[][]::new);
            tuple = e.decodeArgs(topics, data);
        } catch (Exception ex) {
            LOG.info("Fail to decode event, name=" + e.getName(), ex);
            return null;
        }
        TupleType tupleType = e.getInputs();

        return ImmutableList.of(converter.convertTuple(tupleType, tuple));
    }

    public List<Object> decodeFunction(
        final byte[] inputData,
        final byte[] outputData,
        final String functionABI,
        final String functionName
    ) {
        Function f = Function.fromJson(functionABI);
        reviseElementName(f.getInputs(), "");
        reviseElementName(f.getOutputs(), "output");
        if (!f.getName().equals(functionName)) {
            throw new IllegalArgumentException("Function name not match, eventABI=" + functionABI);
        }

        List<Object> values = new LinkedList<>();

        if (f.getInputs().size() > 0) {

            try {
                // See why BytesBuffer is useful:
                // https://github.com/esaulpaugh/headlong/issues/34
                Tuple inputTuple = f.decodeCall(ByteBuffer.wrap(inputData));
                T inputResult = converter.convertTuple(f.getInputs(), inputTuple);
                values.add(inputResult);
            } catch (Exception ex) {
                LOG.info("Fail to decode function, name=" + f.getName(), ex);
                return null;
            }
        }

        if (f.getOutputs().size() > 0) {
            try {
                Tuple outputTuple = f.decodeReturn(ByteBuffer.wrap(outputData));
                T outputResult = converter.convertTuple(f.getOutputs(), outputTuple);
                values.add(outputResult);
            } catch (Exception ex) {
                LOG.info("Fail to decode function, name=" + f.getName(), ex);
                return null;
            }
        }

        return values;
    }

    // The name field in ABI is an optional field, we need to full up all null names:
    // - the field in input: _{i} like: _0, _1
    // - the field in output: output_{i} like: output_0, output_1
    // the logic is same with: https://github.com/datawaves-xyz/blockchain-dbt/blob/master/bdbt/ethereum/abi/abi_transformer.py#L147
    @VisibleForTesting
    static void reviseElementName(TupleType tuple, String prefix) {
        if (tuple.isEmpty()) {
            return;
        }

        for (int i = 0; i < tuple.size(); i++) {
            ABIType<?> element = tuple.get(i);
            if (Strings.isNullOrEmpty(prefix)) {
                setFieldValue(element, "name",
                    Strings.isNullOrEmpty(element.getName())
                        ? "_" + i
                        : element.getName());
            } else {
                setFieldValue(element, "name",
                    Strings.isNullOrEmpty(element.getName())
                        ? prefix + "_" + i
                        : prefix + "_" + element.getName());
            }
        }
    }

    private static void setFieldValue(Object object, String fieldName, Object valueTobeSet) {
        try {
            Field field = getField(object.getClass(), fieldName);
            field.setAccessible(true);
            field.set(object, valueTobeSet);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static Field getField(Class clazz, String fieldName) {
        try {
            return clazz.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                throw new RuntimeException(e);
            } else {
                return getField(superClass, fieldName);
            }
        }
    }
}
