import argparse
import time
import os
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import (
    abs,
    when,
    col,
    lit,
    current_timestamp,
    monotonically_increasing_id,
    row_number,
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.mllib.linalg import Vectors
from xgboost.spark import SparkXGBRegressor

# The label column for prediction
LABEL_COL = "year"


def calculate_accuracy(predictions_df, tolerance):
    """Calculates accuracy within a specified tolerance range."""
    # Ensure prediction and label columns are of a numeric type for calculation
    accuracy_df = predictions_df.withColumn(
        "abs_error",
        abs(col(LABEL_COL).cast(DoubleType()) - col("prediction").cast(DoubleType())),
    ).withColumn("is_correct", when(col("abs_error") <= tolerance, 1.0).otherwise(0.0))

    # Calculate the mean of the 'is_correct' column to get the accuracy
    accuracy_val = accuracy_df.agg({"is_correct": "mean"}).collect()[0][0]
    return accuracy_val


def evaluate_and_print_metrics(
    model_name, predictions_df, training_time, output_path=None, tolerance=5.0
):
    """Evaluates predictions, prints the results, and appends them to a CSV file."""
    # Define evaluators for RMSE and MAE
    evaluator_rmse = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="rmse"
    )
    evaluator_mae = RegressionEvaluator(
        labelCol=LABEL_COL, predictionCol="prediction", metricName="mae"
    )

    # Calculate metrics
    rmse = evaluator_rmse.evaluate(predictions_df)
    mae = evaluator_mae.evaluate(predictions_df)
    accuracy = calculate_accuracy(predictions_df, tolerance)

    # Print results to the console
    print(f"\n--- Evaluation Results for {model_name} ---")
    print(f"Training Time: {training_time:.2f} seconds")
    print(f"Root Mean Squared Error (RMSE): {rmse:.4f}")
    print(f"Mean Absolute Error (MAE):  {mae:.4f}")
    print(f"Accuracy (+/- {tolerance} years): {accuracy:.2%}")

    # Append the results to a CSV file if an output path is provided
    if output_path:
        print(f"Appending evaluation results to {output_path}...")
        try:
            # Create a Spark Session to handle the results DataFrame
            spark = SparkSession.builder.getOrCreate()

            # Structure the results
            result_data = [
                (
                    model_name,
                    training_time,
                    rmse,
                    mae,
                    accuracy,
                    tolerance,
                    str(current_timestamp()),
                )
            ]
            result_schema = StructType(
                [
                    StructField("model_name", DoubleType(), True),
                    StructField("training_time_seconds", DoubleType(), True),
                    StructField("rmse", DoubleType(), True),
                    StructField("mae", DoubleType(), True),
                    StructField("accuracy", DoubleType(), True),
                    StructField("tolerance_years", DoubleType(), True),
                    StructField("timestamp", DoubleType(), True),
                ]
            )
            results_df = spark.createDataFrame(data=result_data, schema=result_schema)

            # Check if the CSV file already exists to decide whether to write a header
            header = "true" if not os.path.exists(output_path) else "false"

            # Append to the CSV file
            results_df.coalesce(1).write.mode("append").option("header", header).csv(
                f"file://{os.path.abspath(output_path)}"
            )

            print("Results appended successfully.")
        except Exception as e:
            print(
                f"Error: Could not append results to {output_path}. Detailed error: {e}"
            )


def run_linear_regression_sgd(training_data, test_data, preproc_stages, args):
    """Trains and evaluates a Linear Regression model with Mini-Batch SGD."""
    print("\n--- Training Linear Regression with Mini-Batch SGD (MLLib) ---")

    # MLLib requires RDD of LabeledPoint, so we need to preprocess and transform
    pipeline = Pipeline(stages=preproc_stages)
    preproc_model = pipeline.fit(training_data)

    train_rdd = (
        preproc_model.transform(training_data)
        .select(LABEL_COL, "features")
        .rdd.map(lambda row: LabeledPoint(row[LABEL_COL], Vectors.dense(row.features)))
    )
    train_rdd.cache()

    test_rdd = (
        preproc_model.transform(test_data)
        .select(LABEL_COL, "features")
        .rdd.map(lambda row: LabeledPoint(row[LABEL_COL], Vectors.dense(row.features)))
    )

    training_time = 0
    if args.mode == "train":
        start_time = time.time()
        # Train the model using SGD
        model = LinearRegressionWithSGD.train(
            train_rdd, iterations=100, step=0.01, miniBatchFraction=0.1
        )
        training_time = time.time() - start_time

        if args.model_output_path:
            print(f"Saving model to {args.model_output_path}...")
            # MLLib models have a different save mechanism
            print(
                "Warning: Standard model saving not supported for MLLib's LinearRegressionWithSGD."
            )
            print("To re-run, you will need to retrain the model.")

    else:  # Load mode
        print(
            "Error: Load mode is not supported for MLLib's LinearRegressionWithSGD in this script."
        )
        return

    # Make predictions
    predictions_rdd = test_rdd.map(
        lambda lp: (float(model.predict(lp.features)), lp.label)
    )
    predictions_df = predictions_rdd.toDF(["prediction", LABEL_COL])

    evaluate_and_print_metrics(
        "Linear Regression (SGD)",
        predictions_df,
        training_time,
        args.output,
        args.tolerance,
    )
    train_rdd.unpersist()


def run_model(
    model_name_str, model_obj, training_data, test_data, preproc_stages, args
):
    """A generic function to run training or loading for pyspark.ml models."""
    print(f"\n--- Handling {model_name_str} Model ---")

    pipeline = Pipeline(stages=preproc_stages + [model_obj])
    training_time = 0

    if args.mode == "train":
        print(f"Training {model_name_str}...")
        start_time = time.time()
        model = pipeline.fit(training_data)
        training_time = time.time() - start_time

        if args.model_output_path:
            try:
                print(f"Saving model to {args.model_output_path}...")
                model.write().overwrite().save(args.model_output_path)
                print("Model saved successfully.")
            except Exception as e:
                print(f"Error saving model to {args.model_output_path}: {e}")

    elif args.mode == "load":
        if not args.model_input_path:
            print("Error: --model-input-path must be provided for load mode.")
            return
        try:
            print(f"Loading model from {args.model_input_path}...")
            model = PipelineModel.load(args.model_input_path)
            print("Model loaded successfully.")
        except Exception as e:
            print(f"Error loading model from {args.model_input_path}: {e}")
            return
    else:
        print(f"Error: Invalid mode '{args.mode}'. Choose 'train' or 'load'.")
        return

    print("Making predictions...")
    # predictions = model.transform(test_data)
    # Repartition the test data to prevent OOM on a single executor during transform
    spark = SparkSession.builder.getOrCreate()
    num_partitions = spark.sparkContext.defaultParallelism * 2
    predictions = model.transform(test_data.repartition(num_partitions))

    evaluate_and_print_metrics(
        model_name_str, predictions, training_time, args.output, args.tolerance
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run regression on Year Prediction MSD dataset with Spark.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # Execution Control
    parser.add_argument(
        "--mode",
        type=str,
        choices=["train", "load"],
        required=True,
        help="Execution mode: 'train' a new model or 'load' a pre-trained one.",
    )
    parser.add_argument(
        "-m",
        "--model",
        type=int,
        required=True,
        choices=[1, 2, 3, 4, 5],
        help="The number for the model to run:\n"
        "  1: Ridge Regression\n"
        "  2: Random Forest\n"
        "  3: GBT (Gradient-Boosted Trees)\n"
        "  4: Mini-Batch Gradient Descent (MLLib)\n"
        "  5: XGBoost Regressor",
    )
    # Path Arguments
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
        default="experiment_results.csv",
        help="Path to append evaluation results to a CSV file (default: experiment_results.csv).",
    )
    parser.add_argument(
        "--model-output-path",
        type=str,
        help="Path to save the trained model (for 'train' mode).",
    )
    parser.add_argument(
        "--model-input-path",
        type=str,
        help="Path to load a pre-trained model from (for 'load' mode).",
    )
    # Model Parameters
    parser.add_argument(
        "-t",
        "--tolerance",
        type=float,
        default=5.0,
        help="Tolerance in years for accuracy calculation (default: 5.0).",
    )

    args = parser.parse_args()

    # Spark Session Initialization
    spark = (
        SparkSession.builder.appName(f"YearPredictionML-{args.model}")
        .config("spark.driver.memory", "10g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Data Loading and Preparation
    feature_cols = [f"feature_{i}" for i in range(90)]
    schema = StructType(
        [StructField(LABEL_COL, DoubleType(), True)]
        + [StructField(col, DoubleType(), True) for col in feature_cols]
    )

    try:
        data = spark.read.csv(
            f"file://{os.path.abspath(args.filepath)}", schema=schema
        ).withColumn("id", monotonically_increasing_id())
    except Exception as e:
        print(f"Error: Could not find or read '{args.filepath}'.")
        print(f"Detailed error: {e}")
        spark.stop()
        return

    window = Window.orderBy(monotonically_increasing_id())
    data_with_row_num = data.withColumn("row_num", row_number().over(window))

    total_count = data_with_row_num.count()
    split_point = int(total_count * 0.9)

    training_data = data_with_row_num.where(col("row_num") <= split_point).drop(
        "row_num"
    )
    test_data = data_with_row_num.where(col("row_num") > split_point).drop("row_num")

    # training_data.cache()
    # test_data.cache()
    # Repartition the training data to distribute the load before caching and training
    num_partitions = spark.sparkContext.defaultParallelism * 4
    training_data = training_data.repartition(num_partitions).cache()
    test_data = test_data.cache()
    print(f"Data loading complete. Total records: {total_count}")
    print(
        f"Training set count: {training_data.count()}, Test set count: {test_data.count()}"
    )

    # Preprocessing
    vector_assembler = VectorAssembler(
        inputCols=feature_cols, outputCol="unscaled_features"
    )
    scaler = StandardScaler(
        inputCol="unscaled_features", outputCol="features", withStd=True, withMean=True
    )
    preprocessing_stages = [vector_assembler, scaler]

    # Model Selection and Execution
    if args.model == 1:
        lr = LinearRegression(
            featuresCol="features",
            labelCol=LABEL_COL,
            regParam=0.1,
            elasticNetParam=0.0,
        )
        run_model(
            "Ridge Regression", lr, training_data, test_data, preprocessing_stages, args
        )
    elif args.model == 2:
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol=LABEL_COL,
            numTrees=50,
            maxDepth=5,
            seed=42,
        )
        run_model(
            "Random Forest", rf, training_data, test_data, preprocessing_stages, args
        )
    elif args.model == 3:
        gbt = GBTRegressor(
            featuresCol="features", labelCol=LABEL_COL, maxIter=100, maxDepth=5, seed=42
        )
        run_model(
            "Gradient-Boosted Tree",
            gbt,
            training_data,
            test_data,
            preprocessing_stages,
            args,
        )
    elif args.model == 4:
        run_linear_regression_sgd(training_data, test_data, preprocessing_stages, args)
    elif args.model == 5:
        xgb = SparkXGBRegressor(
            features_col="features",
            label_col=LABEL_COL,
            n_estimators=100,
            max_depth=5,
            seed=42,
        )
        run_model(
            "XGBoost Regressor",
            xgb,
            training_data,
            test_data,
            preprocessing_stages,
            args,
        )

    # Cleanup
    training_data.unpersist()
    test_data.unpersist()
    spark.stop()
    print("\nTask complete. SparkSession has been stopped.")


if __name__ == "__main__":
    main()
