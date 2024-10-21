package com.bd151876.project1;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.codehaus.jackson.map.MapperConfig;

import java.io.Console;
import java.io.IOException;
import java.util.Objects;

public class NycAccidents extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new NycAccidents(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "NycAccidents");
        job.setJarByClass(this.getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(NycAccidentsMapper.class);
        job.setCombinerClass(NycAccidentsCombiner.class);
        job.setReducerClass(NycAccidentsReducer.class);

        job.setMapOutputKeyClass(MapperKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class NycAccidentsMapper extends Mapper<LongWritable, Text, MapperKey, IntWritable> {
        Boolean[] streets_to_send = {Boolean.FALSE, Boolean.FALSE, Boolean.FALSE};
        String zipCode;
        String[] streets = {"", "", ""};
        int numberOfPedestriansInjured;
        int numberOfPedestriansKilled;
        int numberOfCyclistsInjured;
        int numberOfCyclistsKilled;
        int numberOfMotoristsInjured;
        int numberOfMotoristsKilled;
        public void map(LongWritable offset, Text lineText, Context context) {
            try {

                String line = lineText.toString();
                int i = 0;
                for (String word : line.split(",")) {
                    if (i==0) {
                        if (Integer.parseInt(word.split("/")[2]) < 2012) { return; }
                        break;
                    } // data późniejsza niż 2012
                    if (i==2) {
                        zipCode = word;
                        if (Objects.equals(word, "")) { return; }
                        break;
                    } // zip_code + sprawdzenie czy istnieje
                    if (i==6) {
                        if (!Objects.equals(word, "")) {
                            streets[0] = word;
                            streets_to_send[0] = Boolean.TRUE;
                        }
                    } // on_street_name
                    if (i==7) {
                        if (!Objects.equals(word, "")) {
                            streets[1] = word;
                            streets_to_send[1] = Boolean.TRUE;
                        }
                    } // cross_street_name
                    if (i==8) {
                        if (!Objects.equals(word, "")) {
                            streets[2] = word;
                            streets_to_send[2] = Boolean.TRUE;
                        }
                    } // off_street_name
                    if (i==11) {
                        numberOfPedestriansInjured = Integer.parseInt(word);
                        if (numberOfPedestriansInjured > 0) {
                            this.printuj(numberOfMotoristsKilled, "injured", "pedestrians", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "injured", "pedestrians"),
//                                            new IntWritable(numberOfPedestriansInjured));
//                                    }
//                                }
                        }
                    } // numer_of_pedestrians_injured
                    if (i==12) {
                        numberOfPedestriansKilled = Integer.parseInt(word);
                        if (numberOfPedestriansKilled > 0) {
                            this.printuj(numberOfMotoristsKilled, "killed", "pedestrians", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "killed", "pedestrians"),
//                                            new IntWritable(numberOfPedestriansKilled));
//                                    }
//                                }
                        }
                    } // numer_of_pedestrians_killed
                    if (i==13) {
                        numberOfCyclistsInjured = Integer.parseInt(word);
                        if (numberOfCyclistsInjured > 0) {
                            this.printuj(numberOfMotoristsKilled, "injured", "cyclists", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "injured", "cyclists"),
//                                            new IntWritable(numberOfCyclistsInjured));
//                                    }
//                                }
                        }
                    } // numer_of_cyclist_injured
                    if (i==14) {
                        numberOfCyclistsKilled = Integer.parseInt(word);
                        if (numberOfCyclistsKilled > 0) {
                            this.printuj(numberOfMotoristsKilled, "killed", "cyclists", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "killed", "cyclists"),
//                                            new IntWritable(numberOfCyclistsKilled));
//                                    }
//                                }
                        }
                    } // numer_of_cyclist_killed
                    if (i==15) {
                        numberOfMotoristsInjured = Integer.parseInt(word);
                        if (numberOfMotoristsInjured > 0) {
                            this.printuj(numberOfMotoristsKilled, "injured", "motorists", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "injured", "motorists"),
//                                            new IntWritable(numberOfMotoristsInjured));
//                                    }
//                                }
                        }
                    } // numer_of_motorist_injured
                    if (i==16) {
                        numberOfMotoristsKilled = Integer.parseInt(word);
                        if (numberOfMotoristsKilled > 0) {
                            this.printuj(numberOfMotoristsKilled, "killed", "motorists", context);
//                                for (int j = 0; j < 3; j++) {
//                                    if (streets_to_send[j].equals(Boolean.TRUE)) {
//                                        context.write(
//                                            new MapperKey(zipCode, streets[j], "killed", "motorists"),
//                                            new IntWritable(numberOfMotoristsKilled));
//                                    }
//                                }
                        }
                    } // numer_of_motorist_killed
                    i += 1;
                }
//                    context.write(new MapperKey("siema","eniu","zlamane","kolano"), new IntWritable(1));

            } catch ( Exception e ) {
                e.printStackTrace();
            }
        }

        private void printuj(int val, String dmg, String person, Context cont) throws IOException, InterruptedException {
            for (int j = 0; j < 3; j++) {
                if (streets_to_send[j].equals(Boolean.TRUE)) {
                    cont.write(
                            new MapperKey(zipCode, streets[j], "killed", "motorists"),
                            new IntWritable(numberOfMotoristsKilled));
                }
            }
        }
    }


    public static class NycAccidentsReducer extends Reducer<MapperKey, IntWritable, Text, Text> {
        int total;

        @Override
        public void reduce(MapperKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            total = 0;
            Text resultKey = new Text(key.getStreet() + " " + key.getZipCode() + " " + key.getVictimType() + " " + key.getDmgType() + " ");
            for (IntWritable val : values) {
                total += val.get();
            }
            Text result = new Text(String.valueOf(total));
            context.write(resultKey, result);

        }
    }

    public static class NycAccidentsCombiner extends Reducer<MapperKey, IntWritable, MapperKey, IntWritable> {
        int sum;

        @Override
        public void reduce(MapperKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
