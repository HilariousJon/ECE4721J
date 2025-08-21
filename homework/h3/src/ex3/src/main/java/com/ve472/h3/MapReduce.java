package com.ve472.h3;

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import avro.AvroFileRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MapReduce extends Configured implements Tool {
    public static class BloomMap extends Mapper<AvroKey<AvroFileRecord>, NullWritable, NullWritable, BloomFilter> {
        private static final int VECTOR_SIZE = 1000;
        private static final int HASH_FUNCTION_NUM = 5;
        private BloomFilter bloomFilter;

        @Override
        protected void setup(Context context) {
            bloomFilter = new BloomFilter(VECTOR_SIZE, HASH_FUNCTION_NUM, Hash.MURMUR_HASH);
        }

        @Override
        public void map(AvroKey<AvroFileRecord> key, NullWritable value, Context context)
                throws IOException, InterruptedException {
            String raw = new String(key.datum().getFilecontent().array(), StandardCharsets.UTF_8);
            String[] lines = raw.split("\n");

            for (String line : lines) {
                String[] parts = line.split(",");
                if (parts.length != 3)
                    continue;
                String studentId = parts[1].trim();
                if (studentId.endsWith("3")) {
                    bloomFilter.add(new Key(studentId.getBytes()));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), bloomFilter);
        }
    }

    public static class BloomReduce extends Reducer<NullWritable, BloomFilter, NullWritable, BytesWritable> {
        @Override
        public void reduce(NullWritable key, Iterable<BloomFilter> values, Context context)
                throws IOException, InterruptedException {
            BloomFilter merged = new BloomFilter(1000, 5, Hash.MURMUR_HASH);
            for (BloomFilter bf : values) {
                merged.or(bf);
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            merged.write(dos);
            context.write(NullWritable.get(), new BytesWritable(baos.toByteArray()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(MapReduce.class);
        job.setJobName("BloomFilter Creation Job");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(BloomMap.class);
        job.setReducerClass(BloomReduce.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(BloomFilter.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        AvroJob.setInputKeySchema(job, AvroFileRecord.getClassSchema());

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
