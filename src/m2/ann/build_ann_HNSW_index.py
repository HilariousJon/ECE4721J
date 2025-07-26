import sys
import os
import pickle
import argparse
import numpy as np
import faiss
from pyspark.ml.linalg import VectorUDT
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors
from loguru import logger

logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)
logger.add("ann_build_log_{time}.log", level="DEBUG")

SCALAR_FEATURES_COUNT = 9
TIMBRE_DIMS = 90
TOTAL_DIMS = SCALAR_FEATURES_COUNT + TIMBRE_DIMS


def build_ann_index(
    avro_path: str, output_prefix: str, app_name: str = "SongANNIndexer"
):
    logger.info("--- Starting ANN Index Building Pipeline ---")

    # init spark session
    logger.info("[Step 1/5] Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )
    logger.success("Spark Session created.")

    # load data from spark
    logger.info(f"[Step 2/5] Loading Avro data from: {avro_path}...")
    try:
        song_df = spark.read.format("avro").load(f"file://{os.path.abspath(avro_path)}")
        song_df = song_df.na.fill(0.0, subset=["song_hotttnesss"])
        logger.info("Avro schema:")
        song_df.printSchema()
        song_count = song_df.count()
        logger.success(f"Successfully loaded {song_count} songs.")
    except Exception as e:
        logger.error(f"FATAL: Could not load Avro file. Error: {e}")
        spark.stop()
        sys.exit(1)

    # feature engineering on the data vectors
    logger.info(
        f"[Step 3/5] Performing Feature Engineering to create {TOTAL_DIMS}-dim vectors..."
    )

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

    # Define a UDF to concatenate the 9 scalar features and the 90 timbre features.
    @udf(returnType=ArrayType(DoubleType()))
    def create_99dim_vector(row):
        # Extract scalar features, handling None values
        simple_features = [
            float(row[c]) if row[c] is not None else 0.0 for c in scalar_feature_cols
        ]

        # Get timbre features. Assume it's a list of 90. If not, handle it.
        timbre_data = (
            row["segments_timbre"] if row["segments_timbre"] is not None else []
        )

        # Defensive check: ensure timbre data is exactly 90 dims, pad/truncate if necessary.
        if len(timbre_data) != TIMBRE_DIMS:
            if len(timbre_data) > TIMBRE_DIMS:
                timbre_data = timbre_data[:TIMBRE_DIMS]
            else:
                timbre_data = timbre_data + [0.0] * (TIMBRE_DIMS - len(timbre_data))

        # Concatenate to form the final 99-dim vector
        return simple_features + timbre_data

    # Apply the UDF to create the raw feature vector
    df_with_vector = song_df.withColumn(
        "features_raw",
        create_99dim_vector(struct(*scalar_feature_cols, "segments_timbre")),
    )

    # Convert the Python list from the UDF into a Spark ML Vector, which Normalizer needs
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_ml_vector = df_with_vector.withColumn(
        "features", list_to_vector_udf(col("features_raw"))
    )

    # Use Spark's Normalizer for L2 normalization
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
    vectorized_df = normalizer.transform(df_with_ml_vector).select(
        "track_id", "normFeatures"
    )
    logger.success("Feature engineering complete.")

    # Collecting Data to Driver
    logger.info("[Step 4/5] Collecting vectors to the driver node...")
    local_data = vectorized_df.collect()

    track_ids = [row.track_id for row in local_data]
    feature_vectors = np.array(
        [row.normFeatures.toArray() for row in local_data]
    ).astype("float32")
    vector_mapping = {tid: vec for tid, vec in zip(track_ids, feature_vectors)}
    logger.info(f"Data collected. Shape of vector matrix: {feature_vectors.shape}")

    # Building and Saving Faiss Index & Mappings
    logger.info("[Step 5/5] Building and saving Faiss index and mappings...")

    index_file = f"{output_prefix}_index.ann"
    id_map_file = f"{output_prefix}_id_map.pkl"
    vector_map_file = f"{output_prefix}_vector_map.pkl"

    index = faiss.IndexHNSWFlat(TOTAL_DIMS, 64, faiss.METRIC_L2)
    index.add(feature_vectors)
    logger.info(f"Index built successfully. Total vectors: {index.ntotal}")

    faiss.write_index(index, index_file)
    logger.success(f"Faiss index saved to: '{index_file}'")

    with open(id_map_file, "wb") as f:
        pickle.dump(track_ids, f)
    logger.success(f"Track ID list saved to: '{id_map_file}'")

    with open(vector_map_file, "wb") as f:
        pickle.dump(vector_mapping, f)
    logger.success(f"Track ID -> Vector mapping saved to: '{vector_map_file}'")

    spark.stop()
    logger.info("\n--- Pipeline Finished Successfully! ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a Faiss ANN index from a song Avro file using Spark."
    )
    parser.add_argument(
        "-i", "--input", required=True, type=str, help="Path to the input Avro file."
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=str,
        help="Prefix for the output files (e.g., './my_song_index').",
    )
    args = parser.parse_args()

    build_ann_index(args.input, args.output)
