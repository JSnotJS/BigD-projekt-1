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
    IntWritable pedestrians;
    IntWritable cyclist;
    IntWritable motorist;
    IntWritable killed;
    IntWritable injured;
    public IntWritable getPedestrians() { return pedestrians; }
    public IntWritable getCyclist() { return cyclist; }

    public IntWritable getMotorist() { return motorist; }

    public IntWritable getKilled() { return killed; }

    public IntWritable getInjured() {  return injured; }

    public MapperVal() {
        set(new IntWritable(), new IntWritable(), new IntWritable(), new IntWritable(),  new IntWritable());
    }

    public void set(
            IntWritable pedestrians,
            IntWritable cyclist,
            IntWritable motorist,
            IntWritable injured,
            IntWritable killed) {
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

    public void add(MapperVal val) {
        set(
            new IntWritable(this.pedestrians.get() + val.getPedestrians().get()),
            new IntWritable(this.cyclist.get() + val.getCyclist().get()),
            new IntWritable(this.motorist.get() + val.getMotorist().get()),
            new IntWritable(this.injured.get() + val.getInjured().get()),
            new IntWritable(this.killed.get() + val.getKilled().get())
        );
    }
}
