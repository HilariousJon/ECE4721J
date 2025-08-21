package com.ve472.h3;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ExtractMapReduceOutput {
    public void extract(String avroOutputPath, String outputCsvPath) {
        File inputFile = new File(avroOutputPath);
        if (!inputFile.exists()) {
            System.err.println("Avro file not found: " + avroOutputPath);
            return;
        }

        try {
            GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();

            try (
                    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(inputFile, reader);
                    BufferedWriter writer = new BufferedWriter(
                            new OutputStreamWriter(new FileOutputStream(outputCsvPath), StandardCharsets.UTF_8))) {
                writer.write("studentID,maxScore\n");

                while (dataFileReader.hasNext()) {
                    GenericRecord record = dataFileReader.next();
                    String studentID = record.get("key").toString();
                    String maxScore = record.get("value").toString();
                    writer.write(studentID + "," + maxScore + "\n");
                }

                System.out.println("Extract completed: " + outputCsvPath);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
