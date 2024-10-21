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
    Text zip_code;
    Text victim_type;
    Text dmg_type;

    public MapperKey() {
        set(new Text(), new Text(), new Text(), new Text());
    }
    public MapperKey(String street, String zip_code, String victim_type, String dmg_type) {
        set(
            new Text(street),
            new Text(zip_code),
            new Text(victim_type),
            new Text(dmg_type)
        );
    }
    public void set(Text street, Text zip_code, Text victim_type, Text dmg_type) {
        this.street = street;
        this.zip_code = zip_code;
        this.victim_type = victim_type;
        this.dmg_type = dmg_type;
    }
    public Text getStreet() {
        return street;
    }
    public Text getZipCode() {
        return zip_code;
    }
    public Text getVictimType() { return victim_type; }
    public Text getDmgType() { return dmg_type; }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        street.write(dataOutput);
        zip_code.write(dataOutput);
        victim_type.write(dataOutput);
        dmg_type.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        street.readFields(dataInput);
        zip_code.readFields(dataInput);
        victim_type.readFields(dataInput);
        dmg_type.readFields(dataInput);
    }

    @Override
    public int compareTo(MapperKey o) {
        return 0;
    }
}
