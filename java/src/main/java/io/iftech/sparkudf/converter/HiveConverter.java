package io.iftech.sparkudf.converter;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public class HiveConverter extends Converter<List<Object>> {

    public HiveConverter() {
        super(false);
    }

    @Override
    public Object convertBoolean(Boolean val) {
        return new BooleanWritable(val);
    }

    @Override
    public Object convertByte(Byte val) {
        return new ByteWritable(val);
    }

    @Override
    public Object convertByteArray(byte[] val) {
        return new BytesWritable(val);
    }

    @Override
    public Object convertString(String val) {
        return new Text(val);
    }

    // In Hive, the biggest value of the BIGINT is 9,223,372,036,854,775,807, the biggest precision
    // of the decimal is 38, so there is no data type that can store the 256bit number. For better
    // compatibility, we transform all number type to string type.
    @Override
    public Object convertInt(Integer val) {
        return new Text(val.toString());
    }

    @Override
    public Object convertLong(Long val) {
        return new Text(val.toString());
    }

    @Override
    public Object convertBigInteger(BigInteger val) {
        return new Text(val.toString());
    }

    @Override
    public Object convertBigDecimal(BigDecimal val) {
        return new Text(val.toString());
    }

    @Override
    public Object convertAddress(Address val) {
        return new Text(val.toString().toLowerCase());
    }

    @Override
    public List<Object> convertTuple(TupleType tupleType, Tuple tuple) {
        return convertTupleToObjectList(tupleType, tuple);
    }
}
