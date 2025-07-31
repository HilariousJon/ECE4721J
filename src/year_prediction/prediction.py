import argparse
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import StructType, StructField


def extract_timbre_features(spark, avro_path):
    df = spark.read.format("avro").load(avro_path)

    def split_timbre(timbre_array):
        mean = Vectors.dense(timbre_array[:12])
        cov = Vectors.dense(timbre_array[12:])
        return (mean, cov)

    split_udf = udf(
        split_timbre,
        StructType(
            [
                StructField("timbre_mean", VectorUDT()),
                StructField("timbre_cov", VectorUDT()),
            ]
        ),
    )

    return df.select(
        col("track_id"), col("year"), split_udf(col("segments_timbre")).alias("timbre")
    ).select(
        "track_id",
        "year",
        col("timbre.timbre_mean").alias("timbre_mean"),
        col("timbre.timbre_cov").alias("timbre_cov"),
    )


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Spark Model Prediction with Avro Input"
    )
    parser.add_argument(
        "--model_path",
        type=str,
        required=True,
        help="Path to the saved model directory",
    )
    parser.add_argument(
        "--avro_path", type=str, required=True, help="Path to input Avro file"
    )
    parser.add_argument(
        "--output_path", type=str, required=True, help="Path to save prediction results"
    )
    return parser.parse_args()


def main():
    args = parse_arguments()

    spark = (
        SparkSession.builder.appName("ModelPrediction")
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )

    try:
        model = PipelineModel.load(args.model_path)
        print(f"Successfully loaded model from {args.model_path}")
        input_data = extract_timbre_features(spark, args.avro_path)
        predictions = model.transform(input_data)
        predictions.select("track_id", "year", "prediction").write.mode(
            "overwrite"
        ).parquet(args.output_path)
        print(f"Predictions saved to {args.output_path}")
        print("\nSample predictions:")
        predictions.select("track_id", "year", "prediction").show(5)

    except Exception as e:
        print(f"Error during prediction: {str(e)}")
        raise
    finally:
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    main()
