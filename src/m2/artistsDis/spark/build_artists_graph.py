import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, collect_list, struct
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.linalg import Vectors
import time

def euclidean_dist(v1, v2):
    return float(v1.squared_distance(v2)) ** 0.5

def build_artist_pca(input_path, output_path, k_neighbors=5):
    spark = SparkSession.builder.appName("ArtistGraphBuilder").getOrCreate()

    # Load and clean
    artist_df = (spark.read.format("avro").load(input_path)
        .select("artist_id", "artist_name", "artist_location",
                "artist_latitude", "artist_longitude",
                "artist_hotttnesss", "artist_familiarity")
        .dropna(subset=["artist_id", "artist_hotttnesss", "artist_familiarity", "artist_latitude", "artist_longitude"])
        .dropDuplicates(["artist_id"]))

    # Assemble and scale features
    assembler = VectorAssembler(
        inputCols=["artist_hotttnesss", "artist_familiarity", "artist_latitude", "artist_longitude"],
        outputCol="features"
    )
    features_df = assembler.transform(artist_df)
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features", withStd=True, withMean=True)
    scaled_df = scaler.fit(features_df).transform(features_df)

    # Apply PCA
    start_time = time.time()
    pca = PCA(k=3, inputCol="scaled_features", outputCol="pca_vector")
    pca_model = pca.fit(scaled_df)
    variance = pca_model.explainedVariance
    result_df = pca_model.transform(scaled_df).select("artist_id", "pca_vector")

    # Self join to compute pairwise distances
    dist_udf = udf(euclidean_dist, DoubleType())
    joined = result_df.alias("a").crossJoin(result_df.alias("b")) \
        .filter(col("a.artist_id") != col("b.artist_id")) \
        .withColumn("distance", dist_udf(col("a.pca_vector"), col("b.pca_vector")))

    # Select top-K closest neighbors per artist
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    window = Window.partitionBy("a.artist_id").orderBy("distance")
    topk = joined.withColumn("rank", row_number().over(window)) \
                 .filter(col("rank") <= k_neighbors)

    # Save as adjacency list (artist_id \t [neighbor1,neighbor2,...])
    from pyspark.sql.functions import collect_list

    edges = topk.select(col("a.artist_id").alias("artist_id"), col("b.artist_id").alias("neighbor_id")) \
                .groupBy("artist_id") \
                .agg(collect_list("neighbor_id").alias("neighbors"))
    end_time = time.time()

    edges.write.mode("overwrite").parquet(output_path)

    spark.stop()

    print("Explained variance:", variance)
    print("Graph build time:", end_time - start_time, "seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("-k", "--topk", type=int, default=5)
    args = parser.parse_args()

    build_artist_pca(args.input, args.output, args.topk)