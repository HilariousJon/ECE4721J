package com.ve472.h2;

import avro.AvroFileRecord;

import java.io.File;
import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.avro.Schema;
import java.nio.charset.StandardCharsets;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.generic.GenericDatumWriter;

public class CompactSmallFiles {
    public CompactSmallFiles() {
    }

    public void compactFiles( String inputSchemaPath, String inputDir, String outputAvroFilePath) {
        // load json schema file
        File schemaFile = new File(inputSchemaPath);
        if (!schemaFile.exists()) {
            throw new RuntimeException("Schema file not found: " + inputSchemaPath);
        }
        try {
            // get schema from json
            Schema schema = new Schema.Parser().parse(schemaFile);

            File inputDirectory = new File(inputDir);
            if (!inputDirectory.exists() || !inputDirectory.isDirectory()) {
                throw new RuntimeException("Input directory not found or is not a directory: " + inputDir);
            }
            File[] files = inputDirectory.listFiles();
            File outputAvroFile = new File(outputAvroFilePath);

            GenericDatumWriter<AvroFileRecord> datumWriter = new SpecificDatumWriter<>(AvroFileRecord.class);
            DataFileWriter<AvroFileRecord> writer = new DataFileWriter<>(datumWriter);

            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(schema, outputAvroFile);

            for (File file : files) {
                if (file.isFile()) {
                    List<String> contents = Files.readAllLines(Paths.get(file.getAbsolutePath()));
                    StringBuilder buffer = new StringBuilder();
                    for (String line : contents)
                        buffer.append(line).append("\n");

                    // get whole string content of the file
                    String string = buffer.toString();

                    // get bytes of the file, the ultimate content of the file is store in byte 
                    // it store the actual binary content of the file
                    // ALTERNATIVELY: directly encode the string into byte buffer
                    byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
                    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
                    
                    // following won't work
                    // ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(string);

                    // get checksum of the file
                    String sha256sumString = DigestUtils.sha256Hex(bytes);

                    AvroFileRecord record = AvroFileRecord.newBuilder()
                            .setFilename(file.getName())
                            .setFilecontent(byteBuffer)
                            .setChecksum(sha256sumString)
                            .build();

                    writer.append(record);
                    System.out.println("Compacting file: " + file.getName() + " with checksum: " + sha256sumString);
                }
            }
            writer.close();
            System.out.println("Compaction completed successfully. Output file: " + outputAvroFilePath);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (RuntimeException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }
}
