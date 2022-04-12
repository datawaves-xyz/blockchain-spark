package io.iftech.sparkudf.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Function;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import io.iftech.sparkudf.TestUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import org.apache.spark.sql.Row;
import org.junit.Test;

public class SparkConverterTest {

    private static final Converter<Row> converter = new SparkConverter();

    @Test
    public void testConvertTuple() {
        String functionABIStr = TestUtils.getStringFromFile("/converter/function_abi.json");
        Function f = Function.fromJson(functionABIStr);
        TupleType tupleType = f.getInputs();
        Tuple tuple = Tuple.of(
            Address.wrap("0x7Be8076f4EA4A4AD08075C2508e481d6C946D12b"),
            -100,
            -3000,
            -2000000000,
            -8000000000L,
            100,
            30000,
            2000000000L,
            new BigInteger("8000000000"),
            true,
            BigDecimal.valueOf(-10000.2000),
            BigDecimal.valueOf(10000.2000),
            "hello world".getBytes(StandardCharsets.UTF_8),
            "hi".getBytes(StandardCharsets.UTF_8),
            "This is a test",
            new int[]{1, 2, 3, 4, 5, -8},
            new byte[][]{
                "test1".getBytes(StandardCharsets.UTF_8),
                "test2".getBytes(StandardCharsets.UTF_8)
            },
            new boolean[]{true, true, true, true, true, false, false, false, false, true},
            Tuple.of(
                new BigInteger("12345"),
                "test"
            )
        );

        Row result = converter.convertTuple(tupleType, tuple);

        assertTrue(result.get(0) instanceof String);
        assertEquals("0x7be8076f4ea4a4ad08075c2508e481d6c946d12b", result.get(0).toString());
        assertTrue(result.get(1) instanceof Integer);
        assertEquals(-100, Integer.parseInt(result.get(1).toString()));
        assertTrue(result.get(2) instanceof Integer);
        assertEquals(-3000, Integer.parseInt(result.get(2).toString()));
        assertTrue(result.get(3) instanceof Integer);
        assertEquals(-2000000000, Integer.parseInt(result.get(3).toString()));
        assertTrue(result.get(4) instanceof Long);
        assertEquals(-8000000000L, Long.parseLong(result.get(4).toString()));
        assertTrue(result.get(5) instanceof Integer);
        assertEquals(100, Integer.parseInt(result.get(5).toString()));
        assertTrue(result.get(6) instanceof Integer);
        assertEquals(30000, Integer.parseInt(result.get(6).toString()));
        assertTrue(result.get(7) instanceof Long);
        assertEquals(2000000000L, Long.parseLong(result.get(7).toString()));
        assertTrue(result.get(8) instanceof BigInteger);
        assertEquals(8000000000L, Long.parseLong(result.get(8).toString()));
        assertTrue(result.get(9) instanceof Boolean);
        assertTrue(Boolean.parseBoolean(result.get(9).toString()));
        assertTrue(result.get(10) instanceof BigDecimal);
        assertEquals(-10000.2, Double.parseDouble(result.get(10).toString()), 0.0000001);
        assertTrue(result.get(11) instanceof BigDecimal);
        assertEquals(10000.2, Double.parseDouble(result.get(11).toString()), 0.0000001);
        assertTrue(result.get(12) instanceof byte[]);
        assertTrue(result.get(13) instanceof byte[]);
        assertTrue(result.get(14) instanceof String);
        assertTrue(result.getList(15).get(0) instanceof Integer);
        assertTrue(result.getList(17).get(0) instanceof Boolean);
        assertTrue(result.getStruct(18).get(0) instanceof BigInteger);
        assertTrue(result.getStruct(18).get(1) instanceof String);
    }
}
