import sys
import os
import pickle
import argparse
import numpy as np
import pandas as pd
import shutil
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


def build_ann_index_batched(
    avro_path: str, output_prefix: str, app_name: str = "SongANNIndexer"
):
    logger.info(
        "--- Starting ANN Index Building Pipeline (Memory Conservative Strategy) ---"
    )

    # Spark Session Initialization
    logger.info("[Step 1/4] Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")  # 8g is still fine for processing
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )
    logger.success("Spark Session created.")

    # Data Loading and Feature Engineering
    logger.info(f"[Step 2/4] Loading Avro data and performing Feature Engineering...")
    song_df = spark.read.format("avro").load(f"file://{os.path.abspath(avro_path)}")
    song_df = song_df.na.fill(0.0, subset=["song_hotttnesss"])

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

    @udf(returnType=ArrayType(DoubleType()))
    def create_99dim_vector(row):
        simple_features = [
            float(row[c]) if row[c] is not None else 0.0 for c in scalar_feature_cols
        ]
        timbre_data = (
            row["segments_timbre"] if row["segments_timbre"] is not None else []
        )
        if len(timbre_data) != TIMBRE_DIMS:
            timbre_data = (
                timbre_data[:TIMBRE_DIMS]
                if len(timbre_data) > TIMBRE_DIMS
                else timbre_data + [0.0] * (TIMBRE_DIMS - len(timbre_data))
            )
        return simple_features + timbre_data

    df_with_vector = song_df.withColumn(
        "features_raw",
        create_99dim_vector(struct(*scalar_feature_cols, "segments_timbre")),
    )
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_ml_vector = df_with_vector.withColumn(
        "features", list_to_vector_udf(col("features_raw"))
    )
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
    vectorized_df = normalizer.transform(df_with_ml_vector).select(
        "track_id", "normFeatures"
    )
    logger.success("Feature engineering complete.")

    # Save Processed Vectors to Disk
    logger.info(
        "[Step 3/4] Saving processed vectors to Parquet format to free up Spark memory..."
    )

    # Parquet is a highly efficient columnar storage format. This is a distributed write.
    intermediate_path = "./processed_vectors.parquet"
    vectorized_df.write.mode("overwrite").parquet(intermediate_path)

    spark.stop()
    logger.success(
        f"Spark processing finished. Intermediate data saved to '{intermediate_path}'."
    )

    # process data in batches to avoid memory issues
    logger.info(
        "[Step 4/4] Building Faiss index from Parquet files (outside of Spark)..."
    )

    index_file = f"{output_prefix}_index.ann"
    id_map_file = f"{output_prefix}_id_map.pkl"
    vector_map_file = f"{output_prefix}_vector_map.pkl"

    # Initialize an empty Faiss index
    index = faiss.IndexHNSWFlat(TOTAL_DIMS, 64, faiss.METRIC_L2)

    all_track_ids = []
    vector_mapping = {}

    # Find all the Parquet part-files Spark created.
    part_files = [
        os.path.join(intermediate_path, f)
        for f in os.listdir(intermediate_path)
        if f.endswith(".parquet")
    ]

    for i, file_path in enumerate(part_files):
        logger.debug(f"Processing batch {i+1}/{len(part_files)} from '{file_path}'")
        df_batch = pd.read_parquet(file_path)

        batch_vectors = np.array(df_batch["normFeatures"].tolist()).astype("float32")
        batch_ids = df_batch["track_id"].tolist()

        # Add the vectors from this batch to the index.
        index.add(batch_vectors)

        # Update our mappings.
        all_track_ids.extend(batch_ids)
        for tid, vec in zip(batch_ids, batch_vectors):
            vector_mapping[tid] = vec

    logger.info(f"Index built successfully. Total vectors: {index.ntotal}")

    faiss.write_index(index, index_file)
    logger.success(f"Faiss index saved to: '{index_file}'")

    with open(id_map_file, "wb") as f:
        pickle.dump(all_track_ids, f)
    logger.success(f"Track ID list saved to: '{id_map_file}'")

    with open(vector_map_file, "wb") as f:
        pickle.dump(vector_mapping, f)
    logger.success(f"Track ID -> Vector mapping saved to: '{vector_map_file}'")

    shutil.rmtree(intermediate_path)
    logger.info(f"Cleaned up intermediate directory: '{intermediate_path}'")

    logger.info("\n--- Pipeline Finished Successfully! ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a Faiss ANN index from a song Avro file using a memory-conservative strategy."
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

    build_ann_index_batched(args.input, args.output)
