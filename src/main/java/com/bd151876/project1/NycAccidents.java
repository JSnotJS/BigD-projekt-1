package com.bd151876.project1;
import com.example.bigdata.SumCount;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
public class NycAccidents extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new NycAccidents(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgSizeStations");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NycAccidents.NycAccidentsMapper.class);
//        job.setCombinerClass(NycAccidents.AvgSizeStationCombiner.class);
        job.setReducerClass(NycAccidents.NycAccidentsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class NycAccidentsMapper extends Mapper<LongWritable, Text, MapperKey, MapperVal> {
        private final MapperKey key1 = new MapperKey();
        private final MapperKey key2 = new MapperKey();
        private final MapperKey key3 = new MapperKey();
        private final MapperVal value = new MapperVal();
        Boolean[] streets_to_send = {Boolean.FALSE, Boolean.FALSE, Boolean.FALSE};
        int zipCode;
        String onStreetName;
        String crossStreetName;
        String offStreetName;
        int numberOfPersonsInjured;
        int numberOfPersonsKilled;
        int numberOfPedestriansInjured;
        int numberOfPedestriansKilled;
        int numberOfCyclistsInjured;
        int numberOfCyclistsKilled;
        int numberOfMotoristsInjured;
        int numberOfMotoristsKilled;
        int totalKilledInjured = 0; // liczba poszkodowanych
        int pedestrians = 0; // typ poszkodowanych
        int cyclist = 0; // typ poszkodowanych
        int motorist = 0; // typ poszkodowanych
        int killed = 0; // charakter obrażeń
        int injured = 0; // charakter obrażeń
        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;

                    for (String word : line.split(",")) {
                        if (i == 2) { zipCode = Integer.parseInt(word); } // zip_code
                        if (i == 6) {
                            onStreetName = word;
                            key1.set(new Text(onStreetName), new IntWritable(zipCode));
                            streets_to_send[0] = Boolean.TRUE;
                        } // on_street_name
                        if (i == 7) {
                            crossStreetName = word;
                            key2.set(new Text(onStreetName), new IntWritable(zipCode));
                            streets_to_send[1] = Boolean.TRUE;
                        } // cross_street_name
                        if (i == 8) {
                            offStreetName = word;
                            key3.set(new Text(onStreetName), new IntWritable(zipCode));
                            streets_to_send[2] = Boolean.TRUE;
                        } // off_street_name
                        if (i == 9) {
                            numberOfPersonsInjured = Integer.parseInt(word);

                        } // numer_of_persons_injured
                        if (i == 10) {
                            numberOfPersonsKilled = Integer.parseInt(word);

                        } // numer_of_persons_killed
                        if (i == 11) {
                            numberOfPedestriansInjured = Integer.parseInt(word);
                            pedestrians += numberOfPedestriansInjured;
                        } // numer_of_pedestrians_injured
                        if (i == 12) {
                            numberOfPedestriansKilled = Integer.parseInt(word);
                            pedestrians += numberOfPedestriansKilled;
                        } // numer_of_pedestrians_killed
                        if (i == 13) {
                            numberOfCyclistsInjured = Integer.parseInt(word);
                            cyclist += numberOfCyclistsInjured;
                        } // numer_of_cyclist_injured
                        if (i == 14) {
                            numberOfCyclistsKilled = Integer.parseInt(word);
                            cyclist += numberOfCyclistsKilled;
                        } // numer_of_cyclist_killed
                        if (i == 15) {
                            numberOfMotoristsInjured = Integer.parseInt(word);
                            motorist += numberOfMotoristsInjured;
                        } // numer_of_motorist_injured
                        if (i == 16) {
                            numberOfMotoristsKilled = Integer.parseInt(word);
                            motorist += numberOfMotoristsKilled;
                        } // numer_of_motorist_killed
                        i++;
                    }
                    value.set(
                        new IntWritable(totalKilledInjured),
                        new IntWritable(pedestrians),
                        new IntWritable(cyclist),
                        new IntWritable(motorist),
                        new IntWritable(numberOfPersonsInjured),
                        new IntWritable(numberOfPersonsKilled)
                    );
                }
                if (streets_to_send[0]) { context.write(key1, value); }
                if (streets_to_send[1]) { context.write(key2, value); }
                if (streets_to_send[2]) { context.write(key3, value); }
            } catch ( Exception e) {
                e.printStackTrace();
            }
        }w
    }

    public static class NycAccidentsReducer extends Reducer<MapperKey, Iterable<MapperVal>, Text, DoubleWritable> {
        public void reduce(MapperKey key, Iterable<MapperVal> values, Context context) throws IOException, InterruptedException {
            Text resultKey = new Text("stats for  " + key.getStreet() + " street in '" + key.getZipCode() + "' post area was: ");
            context.write(resultKey, new DoubleWritable(0));

        }
    }

    public static class NycAccidentsCombiner extends Reducer<Text, SumCount, Text, SumCount> {

        private final SumCount sum = new SumCount(0.0d, 0);

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

            sum.set(new DoubleWritable(0.0d), new IntWritable(0));

            for (SumCount val : values) {
                sum.addSumCount(val);
            }
            context.write(key, sum);
        }
    }
}
