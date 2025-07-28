from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("MSD_Timbre_Features").getOrCreate()

df = spark.read.format("avro").load(avro_path)

def split_timbre_features(features):
    timbre_mean = features[:12] # mean
    timbre_cov = features[12:] # covariance
    return (Vectors.dense(timbre_mean), Vectors.dense(timbre_cov))

split_udf = udf(split_timbre_features, 
                returnType=StructType([
                    StructField("timbre_mean", VectorUDT()),
                    StructField("timbre_cov", VectorUDT())
                ]))

processed_df = df.select(
    col("year").alias("label"),
    split_udf(col("segments_timbre")).alias("timbre_features")
).select(
    "label",
    col("timbre_features.timbre_mean").alias("timbre_mean"),
    col("timbre_features.timbre_cov").alias("timbre_cov")
)

processed_df.show(3, truncate=False)
