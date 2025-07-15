import argparse
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, BucketedRandomProjectionLSH
from pyspark.sql.functions import expr

def build_artist_graph(input_path, output_path, k_neighbors=5):
    spark = SparkSession.builder.appName("ArtistGraphWithLSH").getOrCreate()

    # Load and clean
    artist_df = (
        spark.read.format("avro").load(input_path)
        .select("artist_id", "artist_name", "artist_location",
                "artist_latitude", "artist_longitude",
                "artist_hotttnesss", "artist_familiarity")
        .dropna(subset=["artist_id", "artist_hotttnesss", "artist_familiarity", "artist_latitude", "artist_longitude"])
        .dropDuplicates(["artist_id"])
    )

    # Assemble and scale features
    assembler = VectorAssembler(
        inputCols=["artist_hotttnesss", "artist_familiarity", "artist_latitude", "artist_longitude"],
        outputCol="features"
    )
    features_df = assembler.transform(artist_df)

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaled_df = scaler.fit(features_df).transform(features_df)

    # PCA
    start_time = time.time()
    pca = PCA(k=3, inputCol="scaled_features", outputCol="pca_vector")
    pca_model = pca.fit(scaled_df)
    variance = pca_model.explainedVariance
    pca_df = pca_model.transform(scaled_df).select("artist_id", "pca_vector")

    # Use LSH to find approximate nearest neighbors
    lsh = BucketedRandomProjectionLSH(
        inputCol="pca_vector",
        outputCol="hashes",
        bucketLength=1.0,  # can be tuned
        numHashTables=3    # can be tuned
    )
    lsh_model = lsh.fit(pca_df)
    lsh_df = lsh_model.transform(pca_df)

    # Approximate self-join
    joined = lsh_model.approxSimilarityJoin(
        datasetA=pca_df,
        datasetB=pca_df,
        threshold=float("inf"),  # no distance filter here
        distCol="distance"
    ).filter(col("datasetA.artist_id") != col("datasetB.artist_id"))

    # Select top-K nearest neighbors for each artist
    window = Window.partitionBy("datasetA.artist_id").orderBy(col("distance"))
    topk = joined.withColumn("rank", row_number().over(window)) \
                 .filter(col("rank") <= k_neighbors)

    # Build adjacency list: artist_id -> list of neighbor_id
    edges = topk.select(
        col("datasetA.artist_id").alias("artist_id"),
        col("datasetB.artist_id").alias("neighbor_id")
    ).groupBy("artist_id") \
     .agg(collect_list("neighbor_id").alias("neighbors"))

    # Write to parquet
    edges.write.mode("overwrite").parquet(output_path)

    end_time = time.time()
    spark.stop()

    print("Explained variance:", variance)
    print("Graph build time:", end_time - start_time, "seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-k", "--topk", type=int, default=50)
    args = parser.parse_args()

    build_artist_graph(args.input, args.output, args.topk)