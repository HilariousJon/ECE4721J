import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, expr, sha2, concat_ws, udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, BucketedRandomProjectionLSH

def build_song_graph(input_path, output_path, distance_threshold):
    spark = SparkSession.builder.appName("SongGraphWithLSH").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start_time = time.time()

    # Load and clean
    song_df = (
        spark.read.format("avro").load(input_path)
        .select("song_id", "year", "duration", "loudness", "tempo", "energy", "danceability",
                "key", "mode", "time_signature", "song_hotttnesss", "segments_timbre")
        .dropna(subset=["song_id", "year", "duration", "loudness", "tempo", "energy", "danceability",
                        "key", "mode", "time_signature", "song_hotttnesss", "segments_timbre"])
        .dropDuplicates(["song_id"])
    )

    # Flatten 'segments_timbre' to a fixed-length array of 12
    def flatten_timbre(arr):
        if arr is None:
            return [0.0] * 12
        return (arr + [0.0] * 12)[:12]

    print("Flattening segments_timbre...")
    flatten_udf = udf(flatten_timbre, ArrayType(DoubleType()))
    song_df = song_df.withColumn("flat_timbre", flatten_udf(col("segments_timbre")))

    # Expand flat_timbre into 12 separate columns
    for i in range(12):
        song_df = song_df.withColumn(f"timbre_{i}", col("flat_timbre").getItem(i))

    # Assemble features
    feature_cols = ["year", "duration", "loudness", "tempo", "energy", "danceability"
                    , "key", "mode", "time_signature", "song_hotttnesss"] + [f"timbre_{i}" for i in range(12)]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    features_df = assembler.transform(song_df)

    # Standardize
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaled_df = scaler.fit(features_df).transform(features_df)

    # LSH
    print("Building LSH model...")
    lsh = BucketedRandomProjectionLSH(
        inputCol="scaled_features",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=3
    )
    lsh_model = lsh.fit(scaled_df)
    lsh_df = lsh_model.transform(scaled_df)

    # Add bucket ID: use sha256 hash of hash vector
    lsh_df = lsh_df.withColumn("bucket_id", sha2(expr("CAST(hashes[0] AS STRING)"), 256))

    # Get all unique bucket ids
    bucket_ids = [row["bucket_id"] for row in lsh_df.select("bucket_id").distinct().collect()]

    # Perform per-bucket self-join
    print(f"Performing self-join in {len(bucket_ids)} buckets...")
    all_pairs = []
    for bucket_id in bucket_ids:
        bucket_df = lsh_df.filter(col("bucket_id") == bucket_id)
        if bucket_df.count() < 2:
            continue  # skip single-song buckets

        joined = lsh_model.approxSimilarityJoin(
            datasetA=bucket_df,
            datasetB=bucket_df,
            threshold=distance_threshold,
            distCol="distance"
        ).filter(col("datasetA.song_id") != col("datasetB.song_id"))

        all_pairs.append(joined)

    if all_pairs:
        filtered = all_pairs[0]
        for p in all_pairs[1:]:
            filtered = filtered.union(p)
    else:
        filtered = spark.createDataFrame([], lsh_model.approxSimilarityJoin(lsh_df, lsh_df, 1.0, "distance").schema)

    # Build adjacency list
    edges = filtered.select(
        col("datasetA.song_id").cast("string").alias("song_id"),
        col("datasetB.song_id").cast("string").alias("neighbor_id")
    ).groupBy("song_id").agg(collect_list("neighbor_id").alias("neighbors"))

    # Write output
    print("Writing output...")
    edges.write.mode("overwrite").parquet(output_path)

    end_time = time.time()
    spark.stop()
    print("Graph build time:", end_time - start_time, "seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-t", "--threshold", type=float, default=0.5)
    args = parser.parse_args()

    build_song_graph(args.input, args.output, args.threshold)
