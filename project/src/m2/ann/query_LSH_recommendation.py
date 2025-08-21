import argparse
import numpy as np
import sys
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import BucketedRandomProjectionLSHModel
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import col, udf, struct
from loguru import logger

logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)

SCALAR_FEATURES_COUNT = 9
TIMBRE_DIMS = 90
TOTAL_DIMS = SCALAR_FEATURES_COUNT + TIMBRE_DIMS
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


def query_lsh_weighted(
    app_name: str, avro_path: str, model_path: str, weighted_tracks: list, k: int = 10
):
    logger.info("--- Starting LSH Model Query with Lean Memory Strategy ---")

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )

    # Load the LSH model
    logger.info(f"Loading LSH model from {model_path}...")
    lsh_model = BucketedRandomProjectionLSHModel.load(
        f"file://{os.path.abspath(model_path)}"
    )

    # Load the full dataset
    logger.info(f"Loading full dataset from {avro_path}...")
    song_df = (
        spark.read.format("avro")
        .load(f"file://{os.path.abspath(avro_path)}")
        .na.fill(0.0, subset=["song_hotttnesss"])
    )

    # Apply feature engineering
    vectorized_df = song_df.withColumn(
        "normFeatures",
        create_and_normalize_vector(struct(*scalar_feature_cols, "segments_timbre")),
    )

    logger.info(
        "Feature engineering pipeline defined. Caching is disabled for lean memory usage."
    )

    # Create the weighted "Taste Vector"
    logger.info("Creating weighted taste vector from input tracks...")

    input_tracks = {}
    for track_info in weighted_tracks:
        try:
            track_id, weight = track_info.split(":")
            input_tracks[track_id] = float(weight)
        except ValueError:
            logger.error(
                f"Invalid format for --track: '{track_info}'. Please use 'TRACK_ID:WEIGHT'."
            )
            spark.stop()
            return
    input_track_ids = list(input_tracks.keys())

    logger.debug(f"Fetching vectors for tracks: {input_track_ids}")
    input_vectors_df = (
        vectorized_df.filter(col("track_id").isin(input_track_ids))
        .select("track_id", "normFeatures")
        .collect()
    )

    if not input_vectors_df:
        logger.error("None of the input track IDs were found in the dataset.")
        spark.stop()
        return

    # Calculate the weighted average vector on the driver (this is a small, local operation)
    taste_vector = np.zeros(TOTAL_DIMS, dtype="float64")
    total_weight = 0.0
    for row in input_vectors_df:
        track_id = row.track_id
        vector = row.normFeatures.toArray()
        weight = input_tracks[track_id]
        taste_vector += vector * weight
        total_weight += weight

    if total_weight > 0:
        taste_vector /= total_weight

    norm = np.linalg.norm(taste_vector)
    taste_vector_normalized = taste_vector / norm if norm > 0 else taste_vector
    query_vector = Vectors.dense(taste_vector_normalized)
    logger.success("Weighted taste vector created and normalized.")

    # Perform the ANN search
    # Spark will manage memory by streaming data from disk instead of holding it all.
    logger.info(f"Performing ANN search for {k} neighbors...")
    results_df = lsh_model.approxNearestNeighbors(
        vectorized_df, query_vector, k + len(input_track_ids)
    )

    logger.info(f"--- Top {k} Similar Songs to Your Blended Taste (by LSH) ---")
    results = results_df.collect()

    count = 0
    for row in results:
        if row.track_id not in input_track_ids and count < k:
            l2_distance = row.distCol
            cosine_similarity = 1 - (l2_distance**2 / 2)
            print(f"  - Rank {count + 1}:")
            print(f"    Track ID: {row.track_id}")
            print(f"    Title: {row.title}")
            print(f"    Artist: {row.artist_name}")
            print(f"    Cosine Similarity: {cosine_similarity:.4f}\n")
            count += 1

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Query a Spark LSH model to find similar songs based on a weighted taste profile."
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
        "-t",
        "--track",
        required=True,
        action="append",
        help="Input track in 'TRACK_ID:WEIGHT' format. Can be specified multiple times.",
    )
    parser.add_argument(
        "-k", type=int, default=10, help="Number of recommendations to return."
    )
    args = parser.parse_args()

    query_lsh_weighted("SongLSHQuery", args.avro, args.model, args.track, args.k)
