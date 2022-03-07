package io.iftech.sparkudf;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import com.esaulpaugh.headlong.abi.ABIType;
import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.ArrayType;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import com.esaulpaugh.headlong.util.FastHex;

import org.apache.spark.sql.Row;

import scala.collection.JavaConverters;
import scala.collection.Seq;

public class ContractDecoder {
    public static Row buildRowFromTuple(final TupleType tupleType, final Tuple tuple) {

        checkTupleSize(tupleType, tuple);
        List<Object> values = new LinkedList<>();

        for (int i = 0; i < tuple.size(); i++) {

            if (tupleType.get(i).typeCode() == TupleType.TYPE_CODE_TUPLE) {

                values.add(buildRowFromTuple(tupleType.get(i), tuple.get(i)));

            } else if (tupleType.get(i).typeCode() == TupleType.TYPE_CODE_ARRAY) {

                ArrayType<?, Object> arr = tupleType.get(i);
                ABIType<?> elementType = arr.getElementType();

                try {
                    switch (elementType.typeCode()) {
                        // Elementary type array, Headlong will give us a primitive Java array
                        // just pass it to Spark directly without boxing it
                        case ABIType.TYPE_CODE_INT:
                        case ABIType.TYPE_CODE_BIG_INTEGER:
                        case ABIType.TYPE_CODE_BOOLEAN:
                        case ABIType.TYPE_CODE_LONG:
                        case ABIType.TYPE_CODE_BIG_DECIMAL:
                            values.add(tuple.get(i));
                            break;

                        case ABIType.TYPE_CODE_ADDRESS:
                            Address[] addressArray = (Address[]) tuple.get(i);
                            values.add(convertListToSeq(Arrays.stream(addressArray)
                                    .map(x -> x.toString().toLowerCase()).collect(Collectors.toList())));
                            break;

                        case ABIType.TYPE_CODE_BYTE:
                            if (tupleType.get(i).isDynamic()) {
                                // String is treated a variable length byte array underlying
                                values.add(tuple.get(i));
                                break;
                            }

                        case ABIType.TYPE_CODE_ARRAY:
                            // bytes<M>: e.g. bytes4
                            if (tuple.get(i) instanceof byte[]) {
                                values.add(tuple.get(i));
                                break;

                            } else {
                                // e.g. bytes32[4] we treat it as an array of byte array
                                Object[] array = (Object[]) tuple.get(i);
                                values.add(convertListToSeq(
                                        Arrays.stream(array).collect(Collectors.toList())));
                                break;
                            }

                        default:
                            throw new RuntimeException(
                                    "Unhandled ABI Type for array element: " + elementType.getCanonicalType() + "("
                                            + elementType.typeCode() + ")");
                    }
                } catch (RuntimeException ex) {
                    throw new RuntimeException(
                            "Fail to parse array element type " + tupleType.get(i).getCanonicalType() + ", value: "
                                    + tuple.get(i),
                            ex);
                }

            } else {
                values.add(convertElementaryValue(tupleType.get(i), tuple.get(i)));
            }
        }
        return Row.fromSeq(convertListToSeq(values));
    }

    private static Object convertElementaryValue(final ABIType<?> type, final Object val) {

        switch (type.typeCode()) {
            case ABIType.TYPE_CODE_ADDRESS:
                Address addr = (Address) val;
                return addr.toString().toLowerCase();
            case ABIType.TYPE_CODE_INT:
            case ABIType.TYPE_CODE_BIG_INTEGER:
            case ABIType.TYPE_CODE_BOOLEAN:
            case ABIType.TYPE_CODE_LONG:
            case ABIType.TYPE_CODE_BYTE:
            case ABIType.TYPE_CODE_BIG_DECIMAL:

                return val;
            default:
                throw new RuntimeException(
                        "Unhandled elementary ABI Type: " + type.getCanonicalType() + ", code=" + type.typeCode());
        }
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

    private static void checkTupleSize(final TupleType tupleType, final Tuple tuple) {
        int expect = tupleType.size();
        int found = tuple.size();

        if (expect != found) {
            throw new IllegalArgumentException("given Tuple does not match size: expected: " + expect
                    + ", found: " + found + ". (" + tupleType.getName() + ")");
        }
    }

    protected static Seq<Object> convertListToSeq(List<Object> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    protected static Seq<Object> convertListToSeq(Object... it) {
        return convertListToSeq(Arrays.asList(it));
    }
}
