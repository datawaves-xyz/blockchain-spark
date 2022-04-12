package io.iftech.sparkudf;

import com.esaulpaugh.headlong.abi.Event;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import io.iftech.sparkudf.converter.Converter;
import io.iftech.sparkudf.converter.SparkConverter;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
}
