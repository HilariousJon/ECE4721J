import sys
import os
import pickle
import argparse
import pandas as pd
import shutil
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


def build_ann_index_final(
    avro_path: str, output_prefix: str, app_name: str = "SongANNIndexer"
):
    logger.info("--- Starting ANN Index Building Pipeline (Final Memory Strategy) ---")

    CWD = os.getcwd()
    intermediate_path = os.path.join(CWD, "processed_vectors.parquet")
    logger.info(f"Using absolute path for intermediate data: {intermediate_path}")
    # Spark Session Initialization with a more conservative driver memory
    logger.info("[Step 1/5] Initializing Spark Session...")
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.memoryOverheadFactor", "0.2")
        .config("spark.sql.parquet.block.size", 32 * 1024 * 1024)
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )
    logger.success("Spark Session created.")

    logger.info(f"[Step 2/5] Loading Avro data from: {avro_path}...")
    song_df = spark.read.format("avro").load(f"file://{os.path.abspath(avro_path)}")
    song_df = song_df.na.fill(0.0, subset=["song_hotttnesss"])

    num_cores = spark.sparkContext.defaultParallelism
    num_partitions = num_cores * 4
    logger.info(
        f"Detected {num_cores} cores. Repartitioning DataFrame into {num_partitions} partitions to reduce memory pressure per task."
    )
    song_df = song_df.repartition(num_partitions)
    logger.success(f"Successfully loaded and repartitioned {song_df.count()} songs.")

    # --- 3. Feature Engineering ---
    logger.info(f"[Step 3/5] Performing Feature Engineering...")
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

    # Save to Parquet (Memory Conservative)
    logger.info("[Step 4/5] Saving processed vectors to Parquet format...")
    vectorized_df.write.mode("overwrite").parquet(f"file://{intermediate_path}")
    spark.stop()
    logger.success(
        f"Spark processing finished. Intermediate data saved to '{intermediate_path}'."
    )

    # Build Faiss Index in Batches
    logger.info("[Step 5/5] Building Faiss index from Parquet files...")
    index_file = f"{output_prefix}_index.ann"
    id_map_file = f"{output_prefix}_id_map.pkl"
    vector_map_file = f"{output_prefix}_vector_map.pkl"
    index = faiss.IndexHNSWFlat(TOTAL_DIMS, 64, faiss.METRIC_L2)
    all_track_ids = []
    vector_mapping = {}
    part_files = [
        os.path.join(intermediate_path, f)
        for f in os.listdir(intermediate_path)
        if f.endswith(".parquet")
    ]
    for i, file_path in enumerate(part_files):
        logger.debug(f"Processing batch {i+1}/{len(part_files)} from '{file_path}'")
        df_batch = pd.read_parquet(file_path)
        vectors_list = df_batch["normFeatures"].apply(lambda x: x["values"]).tolist()
        batch_vectors = np.array(vectors_list).astype("float32")
        batch_ids = df_batch["track_id"].tolist()
        index.add(batch_vectors)
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
        description="Build a Faiss ANN index from a song Avro file using a final, robust memory strategy."
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
    build_ann_index_final(args.input, args.output)
