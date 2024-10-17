package com.bd151876.project1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapperKey implements WritableComparable<MapperKey> {
    Text street;
    IntWritable zip_code;

    public MapperKey() {
        set(new Text(), new IntWritable());
    }
    public MapperKey(String street, Integer zip_code) {
        set(new Text(street), new IntWritable(zip_code));
    }
    public void set(Text street, IntWritable zip_code) {
        this.street = street;
        this.zip_code = zip_code;
    }
    public Text getStreet() {
        return street;
    }
    public IntWritable getZipCode() {
        return zip_code;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        street.write(dataOutput);
        zip_code.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        street.readFields(dataInput);
        zip_code.readFields(dataInput);
    }

    @Override
    public int compareTo(MapperKey o) {
        return 0;
    }
}
