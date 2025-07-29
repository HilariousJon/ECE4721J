from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import udf

def extract_timbre_features(spark, avro_path):
    df = spark.read.format("avro").load(avro_path)
    
    def split_timbre(timbre_array): # split timbre to 12 mean and 78 cov
        mean = Vectors.dense(timbre_array[:12])
        cov = Vectors.dense(timbre_array[12:])
        return (mean, cov)
    
    split_udf = udf(
        split_timbre,
        StructType([
            StructField("timbre_mean", VectorUDT()),
            StructField("timbre_cov", VectorUDT())
        ])
    )
    
    return df.select(
        col("track_id"),
        col("year"),
        split_udf(col("segments_timbre")).alias("timbre")
    ).select(
        "track_id",
        "year",
        col("timbre.timbre_mean").alias("timbre_mean"),
        col("timbre.timbre_cov").alias("timbre_cov")
    )

if __name__ == "__main__":
    extract_timbre_features()
