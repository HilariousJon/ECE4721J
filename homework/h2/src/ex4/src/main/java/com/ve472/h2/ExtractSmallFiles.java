package com.ve472.h2;

import avro.AvroFileRecord;

import java.io.File;
import java.util.List;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.generic.GenericDatumReader;

public class ExtractSmallFiles {
    public ExtractSmallFiles() {

    }

    public void extract(String inputAvroFilePath, String outputExtractDir) {
        // load the avro file
        File inputAvroFile = new File(inputAvroFilePath);
        if (!inputAvroFile.exists()) {
            throw new RuntimeException("Input Avro file not found: " + inputAvroFilePath);
        }
        // create output directory if it does not exist
        File outputExtractDirectory = new File(outputExtractDir);
        if (!outputExtractDirectory.exists()) {
            outputExtractDirectory.mkdirs();
        }

        try {
            GenericDatumReader<AvroFileRecord> datumReader = new SpecificDatumReader<>(AvroFileRecord.class);
            DataFileReader<AvroFileRecord> reader = new DataFileReader<>(inputAvroFile, datumReader);

            for (AvroFileRecord record: reader) {
                // get the byte buffer from the record
                ByteBuffer byteBuffer = record.getFilecontent();

                // get file name from the record
                String fileName = record.getFilename().toString();

                // decode the file content out of byte buffer
                String decodedContent = StandardCharsets.UTF_8.decode(byteBuffer).toString();

                // get back checksum from the record
                String sha = DigestUtils.sha256Hex(decodedContent);

                // check sha
                if (!sha.equals(record.getChecksum().toString())) {
                    System.err.println("Checksum mismatch for file: " + fileName);
                    System.exit(0);
                }
                
                // write the content to a file in the output directory
                File outputFile = new File(outputExtractDirectory, fileName);
                // directly overwrite
                Files.write(Paths.get(outputFile.getAbsolutePath()), decodedContent.getBytes(StandardCharsets.UTF_8));
                System.out.println("Extracted file: " + outputFile.getAbsolutePath());
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        } 
    }
}
