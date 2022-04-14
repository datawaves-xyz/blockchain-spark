package io.iftech.sparkudf.converter;

import com.esaulpaugh.headlong.abi.ABIType;
import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.ArrayType;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import com.esaulpaugh.headlong.util.FastHex;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.sparkproject.guava.primitives.Booleans;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Convert the headlong ABI type to other type.
 */
public abstract class Converter<T> {

    private final boolean listToSeq;

    public Converter(boolean arrayToSeq) {
        this.listToSeq = arrayToSeq;
    }

    protected abstract Object convertBoolean(Boolean val);

    protected abstract Object convertByte(Byte val);

    protected abstract Object convertByteArray(byte[] val);

    protected abstract Object convertString(String val);

    protected abstract Object convertInt(Integer val);

    protected abstract Object convertLong(Long val);

    protected abstract Object convertBigInteger(BigInteger val);

    protected abstract Object convertBigDecimal(BigDecimal val);

    protected abstract Object convertAddress(Address val);

    public abstract T convertTuple(final TupleType tupleType, final Tuple tuple);

    protected Object convertPrimitiveObject(final ABIType<?> type, final Object val) {
        switch (type.typeCode()) {
            case ABIType.TYPE_CODE_BOOLEAN:
                return this.convertBoolean((Boolean) val);
            case ABIType.TYPE_CODE_BYTE:
                return this.convertByte((Byte) val);
            case ABIType.TYPE_CODE_INT:
                return this.convertInt((Integer) val);
            case ABIType.TYPE_CODE_LONG:
                return this.convertLong((Long) val);
            case ABIType.TYPE_CODE_BIG_INTEGER:
                return this.convertBigInteger((BigInteger) val);
            case ABIType.TYPE_CODE_BIG_DECIMAL:
                return this.convertBigDecimal((BigDecimal) val);
            case ABIType.TYPE_CODE_ADDRESS:
                return this.convertAddress((Address) val);
            default:
                throw new RuntimeException(
                    "Fail to parse primitive type " + type.getCanonicalType() + ", value: " + val);
        }
    }

    protected Object convertArray(final ArrayType<?, Object> type, final Object val) {
        ABIType<?> elementType = type.getElementType();

        List<Object> result;

        switch (elementType.typeCode()) {
            case ABIType.TYPE_CODE_BIG_INTEGER:
            case ABIType.TYPE_CODE_BIG_DECIMAL:
            case ABIType.TYPE_CODE_ADDRESS:
                result = Arrays.stream((Object[]) val)
                    .map(it -> convertPrimitiveObject(elementType, it))
                    .collect(Collectors.toList());
                break;

            case ABIType.TYPE_CODE_BOOLEAN:
                result = Booleans.asList((boolean[]) val).stream()
                    .map(it -> convertPrimitiveObject(elementType, it))
                    .collect(Collectors.toList());
                break;

            case ABIType.TYPE_CODE_INT:
                result = Arrays.stream((int[]) val)
                    .mapToObj(it -> convertPrimitiveObject(elementType, it))
                    .collect(Collectors.toList());
                break;

            case ABIType.TYPE_CODE_LONG:
                result = Arrays.stream((long[]) val)
                    .mapToObj(it -> convertPrimitiveObject(elementType, it))
                    .collect(Collectors.toList());
                break;

            case ABIType.TYPE_CODE_ARRAY:
                result = Arrays.stream((byte[][]) val)
                    .map(it -> convertArray((ArrayType<?, Object>) elementType, it))
                    .collect(Collectors.toList());
                break;

            case ABIType.TYPE_CODE_BYTE:
                return type.getCanonicalType().equals("string")
                    ? convertString((String) val)
                    : convertByteArray((byte[]) val);

            default:
                throw new RuntimeException(
                    "Unhandled ABI Type for array element: "
                        + elementType.getCanonicalType() + "("
                        + elementType.typeCode() + ")"
                );
        }

        if (listToSeq) {
            return convertListToSeq(result);
        } else {
            return result;
        }
    }

    protected List<Object> convertTupleToObjectList(TupleType tupleType, Tuple tuple) {
        checkTupleSize(tupleType, tuple);
        List<Object> values = new LinkedList<>();

        for (int i = 0; i < tuple.size(); i++) {
            if (tupleType.get(i).typeCode() == ABIType.TYPE_CODE_TUPLE) {
                values.add(convertTuple(tupleType.get(i), tuple.get(i)));
            } else if (tupleType.get(i).typeCode() == ABIType.TYPE_CODE_ARRAY) {
                values.add(convertArray(tupleType.get(i), tuple.get(i)));
            } else {
                values.add(convertPrimitiveObject(tupleType.get(i), tuple.get(i)));
            }
        }
        return values;
    }

    public static byte[] decodeHexStartsWith0x(String hex) {
        if (hex == null) {
            return new byte[]{};
        }
        if (hex.startsWith("0x")) {
            return FastHex.decode(hex, 2, hex.length() - 2);
        } else {
            return FastHex.decode(hex);
        }
    }

    public static Seq<Object> convertListToSeq(List<Object> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    private static void checkTupleSize(final TupleType tupleType, final Tuple tuple) {
        int expect = tupleType.size();
        int found = tuple.size();

        if (expect != found) {
            throw new IllegalArgumentException(
                "given Tuple does not match size: expected: " + expect
                    + ", found: " + found + ". (" + tupleType.getName() + ")");
        }
    }
}
