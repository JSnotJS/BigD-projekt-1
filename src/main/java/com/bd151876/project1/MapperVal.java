package com.bd151876.project1;

import com.example.bigdata.SumCount;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapperVal implements WritableComparable<MapperVal> {

    IntWritable totalKilledInjured;
    IntWritable pedestrians;
    IntWritable cyclist;
    IntWritable motorist;
    IntWritable killed;
    IntWritable injured;

    public void set(
            IntWritable totalKilledInjured,
            IntWritable pedestrians,
            IntWritable cyclist,
            IntWritable motorist,
            IntWritable injured,
            IntWritable killed) {
        this.totalKilledInjured = totalKilledInjured;
        this.pedestrians = pedestrians;
        this.cyclist = cyclist;
        this.motorist = motorist;
        this.injured = injured;
        this.killed = killed;
    }
    @Override
    public int compareTo(MapperVal o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
