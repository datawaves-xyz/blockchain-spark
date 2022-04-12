package io.iftech.sparkudf.spark;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.iftech.sparkudf.converter.Converter;
import java.math.BigInteger;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

public class DecodeContractEventUDFTest {

    Gson gson = new GsonBuilder().create();

    @Test
    public void testEventInputsCanDecode() throws Exception {

        ContractEvent e = new ContractEvent("Transfer");
        e.inputs = ImmutableList.of(
            new Field("from", "address", true),
            new Field("to", "address", true),
            new Field("value", "uint256"));

        DecodeContractEventUDF udf = new DecodeContractEventUDF();

        Row row = udf.call(
            Converter.decodeHexStartsWith0x(
                "0x000000000000000000000000000000000000000000003b23f6365b3fabec0000"),
            WrappedArray.make(new String[]{
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000b3f923eabaf178fc1bd8e13902fc5c61d3ddef5b",
                "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60"
            }),
            gson.toJson(e), "Transfer");

        Row inputs = row.getStruct(0);
        assertEquals("0xb3f923eabaf178fc1bd8e13902fc5c61d3ddef5b", inputs.get(0));
        assertEquals("0x28c6c06298d514db089934071355e5743bf21d60", inputs.get(1));
        assertEquals(0,
            (new BigInteger("279283000000000000000000")).compareTo((BigInteger) inputs.get(2)));
    }

    protected static class Field {

        String name;
        String type;
        boolean indexed;

        List<Field> components;

        Field(String name, String type, boolean indexed) {
            this.name = name;
            this.type = type;
            this.indexed = indexed;
        }

        Field(String name, String type) {
            this(name, type, false);
        }
    }

    protected static class ContractEvent {

        String name = "test_event";
        String type = "event";
        List<Field> inputs;

        ContractEvent(String name) {
            this.name = name;
        }
    }
}