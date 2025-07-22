import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, size
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql.functions import udf
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, BucketedRandomProjectionLSH

def build_song_graph(input_path, output_path, distance_threshold):
    spark = SparkSession.builder.appName("SongGraphWithLSH").getOrCreate()

    # Load and clean
    song_df = (
        spark.read.format("avro").load(input_path)
        .select("song_id", "year", "duration", "loudness", "tempo", "energy", "danceability",
                "key", "mode", "time_signature", "song_hotttnesss", "segments_timbre")
        .dropna(subset=["song_id", "year", "duration", "loudness", "tempo", "energy", "danceability",
                        "key", "mode", "time_signature", "song_hotttnesss", "segments_timbre"])
        .dropDuplicates(["song_id"])
    )

    # Determine the max length of segments_timbre
    max_len = song_df.select(size(col("segments_timbre")).alias("len")).agg({"len": "max"}).collect()[0][0]

    # Flatten segments_timbre to length max_len
    def flatten_timbre(arr):
        if arr is None:
            return [0.0] * max_len
        return (arr + [0.0] * max_len)[:max_len]

    flatten_udf = udf(flatten_timbre, ArrayType(DoubleType()))
    song_df = song_df.withColumn("flat_timbre", flatten_udf(col("segments_timbre")))

    # Expand flat_timbre into multiple columns
    for i in range(max_len):
        song_df = song_df.withColumn(f"timbre_{i}", col("flat_timbre").getItem(i))

    # Assemble all feature columns
    feature_cols = ["year", "duration", "loudness", "tempo", "energy", "danceability",
                    "key", "mode", "time_signature", "song_hotttnesss"] + [f"timbre_{i}" for i in range(max_len)]

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    features_df = assembler.transform(song_df)

    # Standardization
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaled_df = scaler.fit(features_df).transform(features_df)

    # PCA
    start_time = time.time()
    pca = PCA(k=15, inputCol="scaled_features", outputCol="pca_vector")
    pca_model = pca.fit(scaled_df)
    variance = pca_model.explainedVariance
    pca_df = pca_model.transform(scaled_df).select("song_id", "pca_vector")

    # LSH
    lsh = BucketedRandomProjectionLSH(
        inputCol="pca_vector",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=3
    )
    lsh_model = lsh.fit(pca_df)
    lsh_df = lsh_model.transform(pca_df)

    # Approximate self-join
    filtered = lsh_model.approxSimilarityJoin(
        datasetA=pca_df,
        datasetB=pca_df,
        threshold=distance_threshold,
        distCol="distance"
    ).filter(col("datasetA.song_id") != col("datasetB.song_id"))

    # Build adjacency list
    edges = filtered.select(
        col("datasetA.song_id").cast("string").alias("song_id"),
        col("datasetB.song_id").cast("string").alias("neighbor_id")
    ).groupBy("song_id") \
     .agg(collect_list("neighbor_id").alias("neighbors"))

    # Write to output
    edges.write.mode("overwrite").parquet(output_path)

    end_time = time.time()
    spark.stop()

    print("Explained variance:", variance)
    print("Graph build time:", end_time - start_time, "seconds")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-t", "--threshold", type=float, default=0.5)
    args = parser.parse_args()

    build_song_graph(args.input, args.output, args.threshold)
