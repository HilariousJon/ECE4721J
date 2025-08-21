# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np


# Initialize Spark Session
spark = (
    SparkSession.builder.appName("CinemaSensorAnalysis")
    .master("local[1]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

num_features = 1000
feature_cols = [f"_c{i}" for i in range(num_features)]
label_col = f"_c{num_features}"

schema = StructType(
    [StructField(col, DoubleType(), True) for col in feature_cols + [label_col]]
)

# Load the datasets
try:
    sensors1_df = spark.read.csv(
        "file:////home/hadoopuser/ece4721-submission/hw/h5/data/sensors1.csv",
        schema=schema,
        header=False,
    )
    sensors2_df = spark.read.csv(
        "file:////home/hadoopuser/ece4721-submission/hw/h5/data/sensors2.csv",
        schema=schema,
        header=False,
    )
except Exception as e:
    print(
        "Error loading data files. Make sure 'sensors1.csv' and 'sensors2.csv' are accessible."
    )
    print(f"Details: {e}")
    spark.stop()
    exit()

# Rename the label column for clarity
sensors1_df = sensors1_df.withColumnRenamed(label_col, "label")
sensors2_df = sensors2_df.withColumnRenamed(label_col, "label")


print("--- Data Loaded Successfully ---")
sensors1_df.printSchema()


# Assemble all feature columns into a single vector
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Scale the features to have zero mean and unit variance
scaler = StandardScaler(
    inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True
)

# We first fit a PCA model with a large number of components to analyze the variance
pca_explainer = PCA(k=200, inputCol="scaledFeatures", outputCol="pcaFeatures")

# Create a temporary pipeline to fit and transform the data for PCA analysis
explainer_pipeline = Pipeline(stages=[assembler, scaler, pca_explainer])
explainer_model = explainer_pipeline.fit(sensors1_df)

# Extract the explained variance vector from the fitted PCA model
explained_variance = explainer_model.stages[-1].explainedVariance

# Calculate the cumulative variance and find n
cumulative_variance = 0.0
n = 0
for i, variance in enumerate(explained_variance):
    cumulative_variance += variance
    if cumulative_variance >= 0.90:
        n = i + 1
        break

if n > 0:
    print(
        f"The number of principal components needed to explain 90% of the variance is: n = {n}"
    )
    print(f"These {n} components explain {cumulative_variance:.2%} of the variance.")
else:
    print(
        f"90% variance not captured with the first {len(explained_variance)} components. Consider increasing 'k' in the PCA explainer."
    )
    # For the script to run, we'll set a default n, but this indicates a need to adjust k
    n = 100
    print(f"Proceeding with a default n = {n}")


# Linear model constructions using PCA features
pca = PCA(k=n, inputCol="scaledFeatures", outputCol="pcaFeatures")

# Define the Linear Regression model
lr = LinearRegression(featuresCol="pcaFeatures", labelCol="label")

# Create the full pipeline: Assembler -> Scaler -> PCA -> Linear Regression
full_pipeline = Pipeline(stages=[assembler, scaler, pca, lr])

# Train the entire pipeline on sensors1.csv data
pipeline_model = full_pipeline.fit(sensors1_df)

print("Linear model trained successfully on sensors1.csv data.")

# Extract the trained linear regression model to inspect its parameters
lr_model = pipeline_model.stages[-1]

print(f"Model Intercept (x_0): {lr_model.intercept}")
# Coefficients are numerous, so we'll show the first 5
print(
    f"Model Coefficients (beta_i) for first 5 components: {lr_model.coefficients[:5]}"
)

# You can also review the model summary on the training data
training_summary = lr_model.summary
print(f"Training Data (sensors1.csv) - R-squared: {training_summary.r2:.4f}")
print(
    f"Training Data (sensors1.csv) - RMSE: {training_summary.rootMeanSquaredError:.4f}"
)


# The key is to use the *exact same pipeline_model* fitted on sensors1.csv
# to transform and make predictions on sensors2.csv.
# This applies the same scaling, same PCA projection, and same regression model.

predictions_df = pipeline_model.transform(sensors2_df)

print("Applied the trained pipeline to sensors2.csv data.")
predictions_df.select("label", "prediction").show(5)

# Evaluate the performance of the model on the new data
evaluator_rmse = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse"
)
evaluator_r2 = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="r2"
)

rmse_on_sensors2 = evaluator_rmse.evaluate(predictions_df)
r2_on_sensors2 = evaluator_r2.evaluate(predictions_df)

print(f"Test Data (sensors2.csv) - R-squared: {r2_on_sensors2:.4f}")
print(f"Test Data (sensors2.csv) - RMSE: {rmse_on_sensors2:.4f}")

# Final Conclusion
print("\n--- Conclusion ---")
if (
    r2_on_sensors2 > 0.7
    and rmse_on_sensors2 < training_summary.rootMeanSquaredError * 1.5
):
    print(
        "The model's performance on sensors2.csv is strong and comparable to its performance on the training data."
    )
    print(
        "This is strong evidence that 'sensors2.csv' VERY LIKELY contains output from the same sensor circuit."
    )
elif r2_on_sensors2 > 0.5:
    print(
        "The model shows some predictive power on sensors2.csv, but performance is degraded."
    )
    print(
        "This suggests 'sensors2.csv' MAY be from the same system, but perhaps under different conditions or with some corruption."
    )
else:
    print(
        "The model's performance on sensors2.csv is poor (low or negative R-squared)."
    )
    print(
        "This is strong evidence that 'sensors2.csv' is LIKELY from a different source or is completely unrelated."
    )

spark.stop()
