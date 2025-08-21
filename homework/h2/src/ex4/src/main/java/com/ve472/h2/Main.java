package com.ve472.h2;

public class Main {
    public static void main(String[] args) {
        String extractDir = "./data/small_extracted_csv";
        String schema = "./schema.json";
        String inputDir = "./data/small_generated_csv";
        String outputAvroFile = "./data/large_compacted_output.avro";

        CompactSmallFiles compactSmallFiles = new CompactSmallFiles();
        compactSmallFiles.compactFiles(schema, inputDir, outputAvroFile);
        ExtractSmallFiles extractSmallFiles = new ExtractSmallFiles();
        extractSmallFiles.extract(outputAvroFile, extractDir);
    }
}
