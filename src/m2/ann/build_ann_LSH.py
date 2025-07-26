import sys
import os
import argparse
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.feature import Normalizer
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.ml.feature import BucketedRandomProjectionLSH
from loguru import logger
from pyspark.ml.linalg import VectorUDT


logger.remove()
logger.add(
    sys.stderr,
    level="INFO",
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
)
logger.add("lsh_build_log_{time}.log", level="DEBUG")

SCALAR_FEATURES_COUNT = 9
TIMBRE_DIMS = 90
TOTAL_DIMS = SCALAR_FEATURES_COUNT + TIMBRE_DIMS


def build_lsh_model(
    avro_path: str, output_model_path: str, app_name: str = "SongLSHModelBuilder"
):
    logger.info("--- Starting Fully Parallel Spark LSH Model Building Pipeline ---")

    # --- 1. Spark Session Initialization ---
    logger.info("[Step 1/3] Initializing Spark Session...")
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

    # --- 2. Data Loading & Feature Engineering (Parallel) ---
    logger.info(
        f"[Step 2/3] Loading Avro data and performing feature engineering in parallel..."
    )
    song_df = spark.read.format("avro").load(f"file://{os.path.abspath(avro_path)}")
    song_df = song_df.na.fill(0.0, subset=["song_hotttnesss"])

    num_cores = spark.sparkContext.defaultParallelism
    num_partitions = num_cores * 4
    logger.info(
        f"Detected {num_cores} cores. Repartitioning DataFrame into {num_partitions} partitions."
    )
    song_df = song_df.repartition(num_partitions)

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
        if len(timbre_data) > TIMBRE_DIMS:
            timbre_data = timbre_data[:TIMBRE_DIMS]
        else:
            timbre_data = timbre_data + [0.0] * (TIMBRE_DIMS - len(timbre_data))
        return simple_features + timbre_data

    df_with_vector = song_df.withColumn(
        "features_raw",
        create_99dim_vector(struct(*scalar_feature_cols, "segments_timbre")),
    )
    list_to_vector_udf = udf(lambda l: Vectors.dense(l), VectorUDT())
    df_with_ml_vector = df_with_vector.withColumn(
        "features", list_to_vector_udf(col("features_raw"))
    )

    # LSH for Euclidean distance works well with L2 normalized vectors.
    normalizer = Normalizer(inputCol="features", outputCol="normFeatures", p=2.0)
    vectorized_df = normalizer.transform(df_with_ml_vector).select(
        "track_id", "title", "artist_name", "normFeatures"
    )

    # Persist the intermediate vectorized DataFrame to avoid re-computation.
    vectorized_df.persist()
    logger.success(
        f"Feature engineering pipeline defined. Total songs to process: {vectorized_df.count()}"
    )

    # --- 3. Build and Save LSH Model (Fully Parallel) ---
    logger.info("[Step 3/3] Building and saving the Spark LSH model in parallel...")

    # Configure the LSH model.
    brp = BucketedRandomProjectionLSH(
        inputCol="normFeatures",
        outputCol="hashes",
        bucketLength=0.1,  # A key tuning parameter. Smaller values mean more buckets, higher precision, but potentially lower recall.
        numHashTables=5,  # More hash tables improve accuracy at the cost of computation and memory.
    )

    lsh_model = brp.fit(vectorized_df)

    # save the model
    lsh_model.write().overwrite().save(f"file://{os.path.abspath(output_model_path)}")

    # Clean up the cached DataFrame.
    vectorized_df.unpersist()

    logger.success(f"LSH model (the index) saved to directory: '{output_model_path}'")
    spark.stop()
    logger.info("\n--- Pipeline Finished Successfully! ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build a Spark LSH model from a song Avro file."
    )
    parser.add_argument(
        "-i", "--input", required=True, type=str, help="Path to the input Avro file."
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        type=str,
        help="Path to save the output LSH model directory.",
    )
    args = parser.parse_args()

    build_lsh_model(args.input, args.output)
