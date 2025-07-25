from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import json
import sys

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(
            "Usage: spark-submit create_input_features.py <path_to_avro_file> <track_id_to_find> <output_filename>",
            file=sys.stderr,
        )
        sys.exit(-1)

    avro_path = sys.argv[1]
    track_id_to_find = sys.argv[2]
    output_filename = sys.argv[3]

    spark = (
        SparkSession.builder.appName("Feature Extractor")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )

    print(f"Reading Avro data from: {avro_path}")
    song_df = spark.read.format("avro").load(avro_path)

    print(f"Filtering for track_id: {track_id_to_find}")

    # Define the feature columns in the exact order needed by the MapReduce job
    feature_cols = [
        "loudness",
        "tempo",
        "duration",
        "energy",
        "danceability",
        "key",
        "mode",
        "time_signature",
        "song_hotttnesss",
    ]

    # Filter for the specific song
    input_song_row = (
        song_df.filter(col("track_id") == track_id_to_find)
        .select(feature_cols + ["segments_timbre"])
        .first()
    )

    if not input_song_row:
        print(
            f"Error: Could not find track_id '{track_id_to_find}' in the data.",
            file=sys.stderr,
        )
        spark.stop()
        sys.exit(1)

    # Build the final feature vector
    final_features = []
    # Add simple features, handling None values
    for feature_name in feature_cols:
        value = input_song_row[feature_name]
        final_features.append(float(value) if value is not None else 0.0)

    # Add timbre features
    timbre_data = input_song_row["segments_timbre"]
    if timbre_data:
        final_features.extend(timbre_data)

    # Write the feature vector to the output JSON file
    with open(output_filename, "w") as f:
        json.dump(final_features, f)

    spark.stop()
    print(f"Successfully created feature vector file: '{output_filename}'")
