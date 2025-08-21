from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithLBFGS
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
)
import os
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# start spark session
spark = SparkSession.builder.appName("Cell Categorizing").getOrCreate()
sc = SparkContext.getOrCreate()

# peak on the dataframe
df = pd.read_csv("../data/PBMC_16k_RNA.csv")
print(df)

print("KLHL17 average:", np.mean(df["KLHL17"]))
print("KLHL17 standard deviance:", np.std(df["KLHL17"]))
print("HES4 average:", np.mean(df["HES4"]))
print("HES4 standard deviance:", np.std(df["HES4"]))

# take into spark dataframe
spark_df = spark.read.options(header="true", inferSchema="true").csv(
    f"file://{os.path.abspath('../data/PBMC_16k_RNA.csv')}"
)

transformed_columns = []
for col in spark_df.columns:
    transformed_columns.append(col.strip().replace(".", ""))
spark_df = spark_df.toDF(*transformed_columns)

spark_df.agg({"KLHL17": "max"}).show()
spark_df.createOrReplaceTempView("data")
spark.sql("SELECT max(KLHL17) FROM data").show()

spark_df.agg({"HES4": "max"}).show()
spark_df.agg({"HES4": "min"}).show()

# merge the features into vector
feature_cols = spark_df.drop("index").columns
feature_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
feature_df = feature_assembler.transform(spark_df).select("index", "features")
feature_df.show(5)

# standardize the feature columns to contribute PCA equally
scaler = StandardScaler(inputCol="features", outputCol="standardized_features")
standardized_df = (
    scaler.fit(feature_df)
    .transform(feature_df)
    .select("index", "standardized_features")
)
standardized_df.show(5)

# Done PCA processing
pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(feature_df)
pca_df = pca_model.transform(feature_df)
pca_df.show(5)
print(pca_model.explainedVariance.toArray())
res_df = (
    pca_df.select("index", "pca_features")
    .rdd.map(lambda x: Row(index=x[0], PC1=float(x[1][0]), PC2=float(x[1][1])))
    .toDF()
)
res_df.show(5)

# data visualization
pd_df = pd.DataFrame(res_df.collect(), columns=res_df.columns)
fig = plt.figure().add_axes([-5, -5, 5, 5])
fig.scatter(pd_df["PC1"], pd_df["PC2"])
plt.savefig("./img/scatter.jpg")
plt.show()
plt.close()

label_df = pd.read_csv("../data/PBMC_16k_RNA_label.csv")
pd_df = pd.merge(pd_df, label_df, how="inner", left_on="index", right_on="index")
original_df = pd.merge(df, label_df, how="inner", left_on="index", right_on="index")

groups = pd_df.groupby("CITEsort")

fig = plt.figure().add_axes([-3, -3, 3, 3])
for name, group in groups:
    fig.plot(group.PC1, group.PC2, marker="o", linestyle="", ms=3, label=name)
fig.legend()
plt.savefig("./img/scatter_with_labels.jpg")
plt.show()
plt.close()

groups = original_df.groupby("CITEsort")
fig = plt.figure().add_axes([-3, -3, 3, 3])
for name, group in groups:
    fig.plot(group.ISG15, group.HES4, marker="o", linestyle="", ms=3, label=name)
fig.legend()
plt.savefig("./img/ISG15_HES4_scatter_with_labels.jpg")
plt.show()
plt.close()

# categorizing using gradient descent
# models using logistic regression, split into two categories as B cell and NON-B cell
schema = StructType(
    [
        StructField("index", StringType(), True),
        StructField("PC1", DoubleType(), True),
        StructField("PC2", DoubleType(), True),
        StructField("CITEsort", StringType(), True),
    ]
)

data_df = spark.createDataFrame(
    pd_df.astype(
        {"PC1": "float64", "PC2": "float64", "CITEsort": "string"}
    ),
    schema=schema,
)
data_df.show(5)

feature_assembler = VectorAssembler(inputCols=["PC1", "PC2"], outputCol="features")
feature_df = feature_assembler.transform(data_df).select(
    "index", "features", "CITEsort"
)
feature_df.show(5)

feature_df.groupBy("CITEsort").count().show()

# divide the dataframe into two categories: B cell and NON-B cell
feature_df.createOrReplaceTempView("data")
t_cell_df = spark.sql("SELECT features FROM data WHERE CITEsort == 'CD4+ T'")
non_t_cell_df = spark.sql("SELECT features FROM data WHERE CITEsort != 'CD4+ T'")
t_cell_df.show(5)
non_t_cell_df.show(5)

# turn into rdd and use regression libraries
t_cell_rdd = t_cell_df.rdd.map(lambda x: LabeledPoint(1, [x[0]]))
non_t_cell_rdd = non_t_cell_df.rdd.map(lambda x: LabeledPoint(0, [x[0]]))
t_cell_rdd.take(5)

# split 80% 20% for traning set and test set
data = t_cell_rdd.union(non_t_cell_rdd)
(training_data, test_data) = data.randomSplit([0.8, 0.2], seed=42)
training_data.cache()
model = LogisticRegressionWithLBFGS.train(training_data, iterations=100)
labels_and_predictions = test_data.map(lambda x: (x.label, model.predict(x.features)))
total = test_data.count()
wrong = labels_and_predictions.filter(lambda res: res[0] != res[1]).count()
error_rate = wrong / float(total)

print("Error rate: ", error_rate)
print("Accuracy: ", 1 - error_rate)
