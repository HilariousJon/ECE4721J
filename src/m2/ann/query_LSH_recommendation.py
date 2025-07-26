import argparse
import numpy as np
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSHModel
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf, struct
from loguru import logger
from pyspark.ml.linalg import VectorUDT


SCALAR_FEATURES_COUNT = 9
TIMBRE_DIMS = 90
scalar_feature_cols = [
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


@udf(returnType=VectorUDT())
def create_and_normalize_vector(row):
    simple_features = [
        float(row[c]) if row[c] is not None else 0.0 for c in scalar_feature_cols
    ]
    timbre_data = row["segments_timbre"] if row["segments_timbre"] is not None else []
    if len(timbre_data) > TIMBRE_DIMS:
        timbre_data = timbre_data[:TIMBRE_DIMS]
    else:
        timbre_data = timbre_data + [0.0] * (TIMBRE_DIMS - len(timbre_data))

    full_vector = np.array(simple_features + timbre_data, dtype=np.float64)
    norm = np.linalg.norm(full_vector)
    normalized_vector = full_vector / norm if norm > 0 else full_vector
    return Vectors.dense(normalized_vector)


def query_lsh(
    app_name: str, avro_path: str, model_path: str, query_track_id: str, k: int = 10
):
    logger.info("--- Starting LSH Model Query ---")

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )

    # Load the distributed LSH model
    logger.info(f"Loading LSH model from {model_path}...")
    lsh_model = BucketedRandomProjectionLSHModel.load(model_path)

    # Load the full dataset to search within
    logger.info(f"Loading full dataset from {avro_path}...")
    song_df = (
        spark.read.format("avro")
        .load(f"file://{os.path.abspath(avro_path)}")
        .na.fill(0.0, subset=["song_hotttnesss"])
    )

    # Apply the same feature engineering to the full dataset
    # In a production environment, this vectorized DataFrame would be saved in Parquet format to speed up loading.
    vectorized_df = song_df.withColumn(
        "normFeatures",
        create_and_normalize_vector(struct(*scalar_feature_cols, "segments_timbre")),
    )
    vectorized_df.persist()

    # Find the query song and extract its vector
    logger.info(f"Finding query vector for track_id: {query_track_id}...")
    query_song_row = vectorized_df.filter(col("track_id") == query_track_id).first()
    if not query_song_row:
        logger.error(f"Query track_id '{query_track_id}' not found in the dataset.")
        spark.stop()
        return
    query_vector = query_song_row.normFeatures

    # Perform the approximate nearest neighbor search
    logger.info(f"Performing ANN search for {k} neighbors...")
    results_df = lsh_model.approxNearestNeighbors(vectorized_df, query_vector, k + 1)

    logger.info(f"--- Top {k} Similar Songs to '{query_song_row.title}' (by LSH) ---")
    results = results_df.collect()

    # Filter out the query song itself and display results
    count = 0
    for row in results:
        if row.track_id != query_track_id and count < k:
            print(f"  - Rank {count + 1}:")
            print(f"    Track ID: {row.track_id}")
            print(f"    Title: {row.title}")
            print(f"    Artist: {row.artist_name}")
            print(f"    Distance (Euclidean): {row.distCol:.4f}\n")
            count += 1

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Query a Spark LSH model to find similar songs."
    )
    parser.add_argument(
        "-a", "--avro", required=True, type=str, help="Path to the original Avro file."
    )
    parser.add_argument(
        "-m",
        "--model",
        required=True,
        type=str,
        help="Path to the saved LSH model directory.",
    )
    parser.add_argument(
        "-q",
        "--query",
        required=True,
        type=str,
        help="The track_id of the song to query.",
    )
    parser.add_argument(
        "-k", type=int, default=10, help="Number of recommendations to return."
    )
    args = parser.parse_args()

    query_lsh("SongLSHQuery", args.avro, args.model, args.query, args.k)
