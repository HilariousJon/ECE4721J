package com.ve472.h3;

import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        String schema = "./schema.json";
        String extractDir = "./data/small_extracted.csv";
        String inputDir = "./data/small_generated_csv";
        String inputAvroFile = "./data/large_compacted_output.avro";
        String outputAvroFile = "./data/output/part-r-00000";
        String hadoopInputPath = "/user/hadoopuser/data/input";
        String hadoopOutputPath = "/user/hadoopuser/data/output";

        CompactSmallFiles compactSmallFiles = new CompactSmallFiles();
        compactSmallFiles.compactFiles(schema, inputDir, inputAvroFile);

        ProcessBuilder pb1 = new ProcessBuilder("bash", "-c",
                "hdfs dfs -put data/large_compacted_output.avro /user/hadoopuser/data/input/large_compacted_output.avro");
        Process process = pb1.start();
        pb1.redirectErrorStream(true);

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Process failed with exit code: " + exitCode);
        }

        MapReduce mapReduce = new MapReduce();
        args = new String[] { hadoopInputPath, hadoopOutputPath };
        int res = ToolRunner.run(mapReduce, args);
        System.exit(res);
    }
}
