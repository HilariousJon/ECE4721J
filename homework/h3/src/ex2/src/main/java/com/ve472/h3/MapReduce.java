package com.ve472.h3;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

// hadoop usage
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;

// avro files
import avro.AvroFileRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;

// mapreduce
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduce extends Configured implements Tool {
    public MapReduce() {
        // Constructor
    }

    public static class AvroMap extends Mapper<AvroKey<AvroFileRecord>, NullWritable, Text, IntWritable> {
        private Text studentID = new Text();
        private IntWritable score = new IntWritable(1);

        @Override
        public void map(AvroKey<AvroFileRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            String rawString = new String(key.datum().getFilecontent().array(), StandardCharsets.UTF_8);
            String[] files = rawString.split("\n");

            for (String line : files) {
                String[] parts = line.split(",");
                if (parts.length != 3) {
                    return;
                }
                this.studentID.set(parts[1]);
                this.score.set(Integer.parseInt(parts[2]));
                context.write(this.studentID, this.score);
            }
        }
    }

    public static class AvroReduce extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            for (IntWritable value : values) {

                if (value.get() >= maxScore) {
                    maxScore = value.get();
                }
            }
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Integer>(maxScore));
        }
    }

    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(MapReduce.class);
        job.setJobName("MapReduce avro files");
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setMapperClass(AvroMap.class);
        job.setReducerClass(AvroReduce.class);
        // job.setCombinerClass(AvroReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // set avro input output schema
        AvroJob.setInputKeySchema(job, AvroFileRecord.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}
