package io.iftech.sparkudf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigInteger;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.spark.sql.Row;
import org.junit.Test;

import scala.collection.mutable.WrappedArray;

public class DecodeContractEventTest {

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
        "0x000000000000000000000000000000000000000000003b23f6365b3fabec0000",
        WrappedArray.make(new String[] {
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x000000000000000000000000b3f923eabaf178fc1bd8e13902fc5c61d3ddef5b",
            "0x00000000000000000000000028c6c06298d514db089934071355e5743bf21d60"
        }),
        gson.toJson(e), "Transfer");

    assertEquals("0xB3f923eaBAF178fC1BD8E13902FC5C61D3DdEF5B", row.get(0));
    assertEquals("0x28C6c06298d514Db089934071355E5743bf21d60", row.get(1));
    assertTrue((new BigInteger("279283000000000000000000")).compareTo((BigInteger) row.get(2)) == 0);
  }

  protected class Field {
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

  protected class ContractEvent {
    String name = "test_event";
    String type = "event";
    List<Field> inputs;

    ContractEvent(String name) {
      this.name = name;
    }
  }
}