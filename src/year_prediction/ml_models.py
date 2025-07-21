import sys
import argparse
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import abs, when, col, lit, current_timestamp
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.linalg import Vectors

LABEL_COL = "year"


def calculate_accuracy(predictions_df, tolerance):
    """Calculates accuracy within a specified tolerance range."""
    accuracy_df = predictions_df.withColumn(
        "abs_error", abs(col(LABEL_COL) - col("prediction"))
    ).withColumn("is_correct", when(col("abs_error") <= tolerance, 1.0).otherwise(0.0))
    accuracy_val = accuracy_df.agg({"is_correct": "mean"}).collect()[0][0]
    return accuracy_val


def evaluate_and_print_metrics(
    model_name, predictions_df, output_path=None, tolerance=5.0
):
    """Evaluates predictions, prints the results, and optionally appends them to a CSV file."""
    evaluator_rmse = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="mae"
    )

    rmse = evaluator_rmse.evaluate(predictions_df)
    mae = evaluator_mae.evaluate(predictions_df)
    accuracy = calculate_accuracy(predictions_df, tolerance)

    print(f"\n--- Evaluation Results for {model_name} ---")
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
    print(f"Mean Absolute Error (MAE):  {mae:.4f}")
    print(f"Accuracy (+/- {tolerance} years): {accuracy:.2%}")

    # Append the results to a CSV file if an output path is provided
    if output_path:
        print(f"Appending prediction results to {output_path}...")
        try:
            # Add metadata columns for a more descriptive log format
            output_df_with_metadata = predictions_df.withColumn(
                "model_name", lit(model_name)
            ).withColumn("timestamp", current_timestamp())

            # Select and rename columns for the final output format
            output_df_formatted = output_df_with_metadata.select(
                col("timestamp"),
                col("model_name"),
                col(LABEL_COL).alias("true_year"),
                col("prediction").alias("predicted_year"),
            ).withColumn(
                "absolute_error", abs(col("true_year") - col("predicted_year"))
            )

            # Append to the CSV file. No header is written to avoid duplicates on append.
            output_df_formatted.coalesce(1).write.mode("append").csv(output_path)

            print("Results appended successfully.")
            print(f"Column order: {', '.join(output_df_formatted.columns)}")
        except Exception as e:
            print(
                f"Error: Could not append results to {output_path}. Detailed error: {e}"
            )


def run_ridge_regression(
    training_data, test_data, preproc_stages, output_path, tolerance
):
    """Trains and evaluates a Ridge Regression model."""
    print("\n--- Training Ridge Regression Model ---")
    lr = LinearRegression(
        featuresCol="features", labelCol=LABEL_COL, regParam=0.1, elasticNetParam=0.0
    )
    pipeline = Pipeline(stages=preproc_stages + [lr])
    model = pipeline.fit(training_data)
    predictions = model.transform(test_data)
    evaluate_and_print_metrics("Ridge Regression", predictions, output_path, tolerance)


def run_random_forest(training_data, test_data, preproc_stages, output_path, tolerance):
    """Trains and evaluates a Random Forest Regressor model."""
    print("\n--- Training Random Forest Model ---")
    rf = RandomForestRegressor(
        featuresCol="features", labelCol=LABEL_COL, numTrees=100, maxDepth=10, seed=42
    )
    pipeline = Pipeline(stages=preproc_stages + [rf])
    model = pipeline.fit(training_data)
    predictions = model.transform(test_data)
    evaluate_and_print_metrics("Random Forest", predictions, output_path, tolerance)


def run_gbt(training_data, test_data, preproc_stages, output_path, tolerance):
    """Trains and evaluates a Gradient-Boosted Tree Regressor model."""
    print("\n--- Training Gradient-Boosted Tree (GBT) Model ---")
    gbt = GBTRegressor(
        featuresCol="features", labelCol=LABEL_COL, maxIter=100, maxDepth=5, seed=42
    )
    pipeline = Pipeline(stages=preproc_stages + [gbt])
    model = pipeline.fit(training_data)
    predictions = model.transform(test_data)
    evaluate_and_print_metrics(
        "Gradient-Boosted Tree (GBT)", predictions, output_path, tolerance
    )


def run_linear_regression_ml(
    training_data, test_data, preproc_stages, output_path, tolerance
):
    """
    Trains and evaluates a Linear Regression model using the modern pyspark.ml API.
    """
    print("\n--- Training Linear Regression Model (ML API) ---")
    
    lr = LinearRegression(featuresCol="features", labelCol=LABEL_COL)

    # The entire process is clean and operates on DataFrames.
    pipeline = Pipeline(stages=preproc_stages + [lr])

    model = pipeline.fit(training_data)

    predictions = model.transform(test_data)

    evaluate_and_print_metrics(
        "Linear Regression (ML API)", predictions, output_path, tolerance
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run a regression model on the Year Prediction MSD dataset using Spark.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "-m",
        "--model",
        type=int,
        choices=[1, 2, 3, 4],
        help="The number for the model to run:\n"
        "  1: Ridge Regression\n"
        "  2: Random Forest\n"
        "  3: GBT (Gradient-Boosted Trees)\n"
        "  4: Mini-Batch Gradient Descent",
    )
    parser.add_argument(
        "-i",
        "--filepath",
        type=str,
        default="YearPredictionMSD.txt",
        help="Path to the input data file (default: YearPredictionMSD.txt).",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Optional path to append the prediction results to a CSV file.",
    )
    parser.add_argument(
        "-t",
        "--tolerance",
        type=float,
        default=5.0,
        help="Tolerance in years for accuracy calculation (default: 5.0).",
    )

    args = parser.parse_args()
    model_number = args.model
    filepath = args.filepath
    output_path = args.output
    tolerance = args.tolerance

    model_map = {
        1: ("Ridge", run_ridge_regression),
        2: ("RandomForest", run_random_forest),
        3: ("GBT", run_gbt),
        4: ("MiniBatchGD", run_linear_regression_ml),
    }

    model_name, model_func = model_map[model_number]

    spark = (
        SparkSession.builder.appName(f"YearPredictionML-{model_name}")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    feature_cols = [f"feature_{i}" for i in range(90)]
    schema = StructType(
        [StructField(LABEL_COL, DoubleType(), True)]
        + [StructField(col, DoubleType(), True) for col in feature_cols]
    )

    try:
        data = spark.read.csv("file:///" + filepath, schema=schema)
    except Exception as e:
        print(f"Error: Could not find or read '{filepath}'.")
        print("Please ensure the file path is correct.")
        print(f"Detailed error: {e}")
        spark.stop()
        return

    vector_assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="unscaled_features"
    )
    scaler = StandardScaler(
        inputCol="unscaled_features", outputCol="features", withStd=True, withMean=True
    )
    preprocessing_stages = [vector_assembler, scaler]

    (training_data, test_data) = data.randomSplit([0.8, 0.2], seed=42)
    training_data.cache()
    test_data.cache()
    print(
        f"Data loading complete. Training set count: {training_data.count()}, Test set count: {test_data.count()}"
    )

    model_func(training_data, test_data, preprocessing_stages, output_path, tolerance)

    training_data.unpersist()
    test_data.unpersist()
    spark.stop()
    print("\nTask complete. SparkSession has been stopped.")


if __name__ == "__main__":
    main()
