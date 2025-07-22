import argparse
import time
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField

from pyspark.ml.feature import VectorAssembler, BucketedRandomProjectionLSH

def build_song_graph(input_path, output_path, distance_threshold):
    spark = SparkSession.builder.appName("SongGraphWithMahalanobisLSH").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start_time = time.time()

    # Load dataset (Avro format), select and clean required columns
    song_df = (
        spark.read.format("avro").load(input_path)
        .select("song_id", "segments_timbre")
        .dropna(subset=["song_id", "segments_timbre"])
        .dropDuplicates(["song_id"])
    )

    # ----------------------------
    # Step 1: Parse feature vector and covariance
    # ----------------------------

    # segments_timbre[0:12] is the song vector
    # segments_timbre[12:90] is the upper triangle of 12x12 covariance matrix

    def parse_segments(arr):
        vec = (arr[:12] if arr else [0.0]*12)
        cov_upper = (arr[12:90] if arr and len(arr) >= 90 else [0.0]*78)
        return vec, cov_upper

    print("Parsing segments...")
    parse_udf = udf(lambda x: parse_segments(x), StructType([
        StructField("vec", ArrayType(DoubleType())),
        StructField("cov_upper", ArrayType(DoubleType()))
    ]))

    song_df = song_df.withColumn("parsed", parse_udf(col("segments_timbre")))
    song_df = song_df.withColumn("vec", col("parsed.vec"))
    song_df = song_df.withColumn("cov_upper", col("parsed.cov_upper"))

    # ----------------------------
    # Step 2: Convert covariance vector to matrix and compute Σ^{-1/2}
    # ----------------------------

    def cov_upper_to_full(cov_upper):
        cov = np.zeros((12, 12))
        idx = 0
        for i in range(12):
            for j in range(i, 12):
                cov[i, j] = cov_upper[idx]
                cov[j, i] = cov_upper[idx]
                idx += 1
        return cov

    print("Computing covariance matrix...")
    sample_cov_upper = song_df.select("cov_upper").first()["cov_upper"]
    cov = cov_upper_to_full(sample_cov_upper)

    # Regularize in case of singular matrix
    cov += np.eye(12) * 1e-6

    # Compute Σ^{-1/2} via Cholesky
    cov_inv_sqrt = np.linalg.inv(np.linalg.cholesky(cov)).T

    # Broadcast the inverse square root to workers
    broadcast_inv_sqrt = spark.sparkContext.broadcast(cov_inv_sqrt)

    # ----------------------------
    # Step 3: Whiten feature vectors using Σ^{-1/2}
    # ----------------------------

    def whiten(vec):
        return (broadcast_inv_sqrt.value @ np.array(vec)).tolist()

    print("Whitening feature vectors...")
    whiten_udf = udf(whiten, ArrayType(DoubleType()))
    song_df = song_df.withColumn("whitened", whiten_udf(col("vec")))

    # Expand whitened vector into columns
    for i in range(12):
        song_df = song_df.withColumn(f"feature_{i}", col("whitened").getItem(i))

    # ----------------------------
    # Step 4: LSH with whitened vectors
    # ----------------------------

    print("Assembling features for LSH...")
    feature_cols = [f"feature_{i}" for i in range(12)]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    assembled_df = assembler.transform(song_df)

    # Apply LSH
    print("Building LSH model...")
    lsh = BucketedRandomProjectionLSH(
        inputCol="features",
        outputCol="hashes",
        bucketLength=1.0,
        numHashTables=3
    )
    lsh_model = lsh.fit(assembled_df)
    lsh_df = lsh_model.transform(assembled_df)

    # ----------------------------
    # Step 5: LSH Approximate Join + Filter by Distance
    # ----------------------------

    print("Finding approximate neighbors...")
    joined = lsh_model.approxSimilarityJoin(
        datasetA=lsh_df,
        datasetB=lsh_df,
        threshold=distance_threshold,
        distCol="distance"
    ).filter(col("datasetA.song_id") != col("datasetB.song_id"))

    # ----------------------------
    # Step 6: Build adjacency list and write output
    # ----------------------------

    print("Building adjacency list...")
    edges = joined.select(
        col("datasetA.song_id").cast("string").alias("song_id"),
        col("datasetB.song_id").cast("string").alias("neighbor_id")
    ).groupBy("song_id") \
     .agg(collect_list("neighbor_id").alias("neighbors"))

    edges.write.mode("overwrite").parquet(output_path)

    end_time = time.time()
    print(f"Graph built in {end_time - start_time:.2f} seconds")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-t", "--threshold", type=float, default=0.5)
    args = parser.parse_args()

    build_song_graph(args.input, args.output, args.threshold)
