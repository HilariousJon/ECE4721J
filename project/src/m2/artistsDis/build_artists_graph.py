import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list
from pyspark.ml.feature import (
    VectorAssembler,
    StandardScaler,
    PCA,
    BucketedRandomProjectionLSH,
)
from pyspark.sql.functions import expr, sha2


def build_artist_graph(input_path, output_path, distance_threshold):
    spark = SparkSession.builder.appName("ArtistGraphWithLSH").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    start_time = time.time()

    # Load and clean
    artist_df = (
        spark.read.format("avro")
        .load(input_path)
        .select(
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
            "artist_hotttnesss",
            "artist_familiarity",
        )
        .dropna(
            subset=[
                "artist_id",
                "artist_hotttnesss",
                "artist_familiarity",
                "artist_latitude",
                "artist_longitude",
            ]
        )
        .dropDuplicates(["artist_id"])
    )

    # Assemble and scale features
    assembler = VectorAssembler(
        inputCols=[
            "artist_hotttnesss",
            "artist_familiarity",
            "artist_latitude",
            "artist_longitude",
        ],
        outputCol="features",
    )
    features_df = assembler.transform(artist_df)

    scaler = StandardScaler(
        inputCol="features", outputCol="scaled_features", withStd=True, withMean=True
    )
    scaled_df = scaler.fit(features_df).transform(features_df)

    # PCA
    print("Performing PCA...")
    pca = PCA(k=3, inputCol="scaled_features", outputCol="pca_vector")
    pca_model = pca.fit(scaled_df)
    variance = pca_model.explainedVariance
    pca_df = pca_model.transform(scaled_df).select("artist_id", "pca_vector")

    # LSH
    print("Building LSH model...")
    lsh = BucketedRandomProjectionLSH(
        inputCol="pca_vector", outputCol="hashes", bucketLength=1.0, numHashTables=3
    )
    lsh_model = lsh.fit(pca_df)
    lsh_df = lsh_model.transform(pca_df)

    # Add bucket ID based on hashes[0]
    lsh_df = lsh_df.withColumn(
        "bucket_id", sha2(expr("CAST(hashes[0] AS STRING)"), 256)
    )

    # Get unique bucket IDs
    bucket_ids = [
        row["bucket_id"] for row in lsh_df.select("bucket_id").distinct().collect()
    ]

    # Self-join within each bucket
    print(f"Finding approximate neighbors in {len(bucket_ids)} buckets...")
    all_pairs = []
    for bucket_id in bucket_ids:
        bucket_df = lsh_df.filter(col("bucket_id") == bucket_id)
        if bucket_df.count() < 2:
            continue

        joined = lsh_model.approxSimilarityJoin(
            datasetA=bucket_df,
            datasetB=bucket_df,
            threshold=distance_threshold,
            distCol="distance",
        ).filter(col("datasetA.artist_id") != col("datasetB.artist_id"))

        all_pairs.append(joined)

    if all_pairs:
        filtered = all_pairs[0]
        for p in all_pairs[1:]:
            filtered = filtered.union(p)
    else:
        filtered = spark.createDataFrame(
            [], lsh_model.approxSimilarityJoin(lsh_df, lsh_df, 1.0, "distance").schema
        )

    # Build adjacency list
    print("Building adjacency list...")
    edges = (
        filtered.select(
            col("datasetA.artist_id").cast("string").alias("artist_id"),
            col("datasetB.artist_id").cast("string").alias("neighbor_id"),
        )
        .groupBy("artist_id")
        .agg(collect_list("neighbor_id").alias("neighbors"))
    )

    # Write output
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

    build_artist_graph(args.input, args.output, args.threshold)
