package com.ve472.h3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Main {
    public static void main(String[] args) {
        args = new String[] {"data/input/student_1000.csv", "data/output"};
        try {
            driver(args);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public static class Map extends Mapper<Object, Text, Text, IntWritable> {
        private Text studentID = new Text();
        private IntWritable score = new IntWritable(0);

        // The Mapper implementation, via the map method, processes one line at a time
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length != 3) {
                return;
            }
            this.studentID.set(fields[1]);
            this.score.set(Integer.parseInt(fields[2]));
            context.write(this.studentID, this.score);
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxGrade = 0;
            for (IntWritable val : values) {
                if (val.get() > maxGrade) {
                    maxGrade = val.get();
                }
            }
            this.result.set(maxGrade);
            context.write(key, this.result);
        }
    }

    public static void driver(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Grade");

        job.setJarByClass(Main.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set input and output paths
        if (args.length != 2) {
            System.err.println("Usage: Main <input path> <output path>");
            System.exit(-1);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // savely close the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
