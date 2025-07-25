from pyspark.sql import SparkSession
import sys
import os

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: spark-submit create_song_data.py <path_to_avro_file> <output_path_for_json>",
            file=sys.stderr,
        )
        sys.exit(-1)

    avro_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder.appName("Avro to JSONL Converter")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )
    local_avro_path = f"file://{os.path.abspath(avro_path)}"
    print(f"Reading Avro data from: {local_avro_path}")

    # Read the original Avro file
    song_df = spark.read.format("avro").load(local_avro_path)

    print(f"Writing data as JSON to: {output_path}")

    # Write the DataFrame as JSON. Spark will create a directory with part-files.
    # This format is exactly what JSON Lines is.
    song_df.write.mode("overwrite").json(f"file://{output_path}")

    spark.stop()
    print("Conversion successful.")
    print(f"Your JSONL data is in the directory: {output_path}")
