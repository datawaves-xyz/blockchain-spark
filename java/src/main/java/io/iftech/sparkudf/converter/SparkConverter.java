package io.iftech.sparkudf.converter;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.spark.sql.Row;

public class SparkConverter extends Converter<Row> {

    public SparkConverter() {
        super(true);
    }

    @Override
    public Object convertBoolean(Boolean val) {
        return val;
    }

    @Override
    public Object convertByte(Byte val) {
        return val;
    }

    @Override
    public Object convertByteArray(byte[] val) {
        return val;
    }

    @Override
    public Object convertString(String val) {
        return val;
    }

    @Override
    public Object convertInt(Integer val) {
        return val;
    }

    @Override
    public Object convertLong(Long val) {
        return val;
    }

    @Override
    public Object convertBigInteger(BigInteger val) {
        return val;
    }

    @Override
    public Object convertBigDecimal(BigDecimal val) {
        return val;
    }

    @Override
    public Object convertAddress(Address val) {
        return val.toString().toLowerCase();
    }

    @Override
    public Row convertTuple(TupleType tupleType, Tuple tuple) {
        return Row.fromSeq(Converter.convertListToSeq(convertTupleToObjectList(tupleType, tuple)));
    }
}
