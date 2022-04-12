package io.iftech.sparkudf.converter;

import com.esaulpaugh.headlong.abi.Address;
import com.esaulpaugh.headlong.abi.Tuple;
import com.esaulpaugh.headlong.abi.TupleType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class HiveConverter extends Converter<List<Object>> {

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

    @Override
    public Object convertInt(Integer val) {
        return new IntWritable(val);
    }

    @Override
    public Object convertLong(Long val) {
        return new LongWritable(val);
    }

    @Override
    public Object convertBigInteger(BigInteger val) {
        return new HiveDecimalWritable(HiveDecimal.create(val));
    }

    @Override
    public Object convertBigDecimal(BigDecimal val) {
        return new HiveDecimalWritable(HiveDecimal.create(val));
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
