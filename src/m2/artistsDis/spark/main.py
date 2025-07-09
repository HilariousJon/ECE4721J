from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, array
from pyspark.sql.types import DoubleType
from graphframes import GraphFrame
import math

def geo_dist(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]:
        return float("inf")
    return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)

def get_neighbors(dataPath, start_id, end_id):
    """
    Get neighbors of an artist using BFS in a GraphFrame.
    :param dataPath: Path to Avro file
    :param start_id: Starting artist_id
    :param end_id: Target artist_id
    :return: List of artist_ids along path from start to end (if found)
    """
    spark = SparkSession.builder \
    .appName("Get Neighbors") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-avro_2.12:3.2.4,"
            "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
    .getOrCreate()

    # Load and clean data
    artist_df = spark.read.format("avro").load(dataPath)
    artist_df = artist_df.select(
        "artist_id", "artist_name", "artist_location",
        "artist_latitude", "artist_longitude",
        "artist_hotttnesss", "artist_familiarity"
    ).dropna(subset=["artist_id"])

    # Create vertex DataFrame
    vertices_df = artist_df.select(col("artist_id").alias("id"))

    # Create edges based on proximity/location/hotttnesss/familiarity
    joined = artist_df.alias("a").join(
        artist_df.alias("b"),
        col("a.artist_id") < col("b.artist_id")  # avoid self-loop and duplicate
    )

    # Add distance column
    udf_geo_dist = udf(geo_dist, DoubleType())
    edges = joined.withColumn("distance", udf_geo_dist(
        col("a.artist_latitude"), col("a.artist_longitude"),
        col("b.artist_latitude"), col("b.artist_longitude")
    )).filter(
        (col("a.artist_location") == col("b.artist_location")) |
        (col("distance") < 1.0) |
        (
            (col("a.artist_hotttnesss") < 0.1) & (col("b.artist_hotttnesss") < 0.1)
        ) |
        (
            (col("a.artist_familiarity") < 0.1) & (col("b.artist_familiarity") < 0.1)
        )
    )

    # Create edge DataFrame
    edges_df = edges.select(
        col("a.artist_id").alias("src"),
        col("b.artist_id").alias("dst")
    )

    # Build GraphFrame
    graph = GraphFrame(vertices_df, edges_df)

    # Run BFS
    result = graph.bfs(
        fromExpr=f"id = '{start_id}'",
        toExpr=f"id = '{end_id}'",
        maxPathLength=5
    )

    # Extract path (list of intermediate artist IDs)
    # neighbors = result.select(array([col for col in result.columns if col.startswith("e")]).alias("neighbors")).collect()

    spark.stop()

    paths = result.collect()
    if not paths:
        return -1

    distance = sum(1 for key in paths[0].asDict() if key.startswith("e"))
    return distance

if __name__ == "__main__":
    dataPath = "src/data/songs.avro"
    start_id = "AR1W6QX1187B9A3F4C"
    end_id = "AR1W6QX1187B9A3F4D"
    
    distance = get_neighbors(dataPath, start_id, end_id)
    print(f"Distance from {start_id} to {end_id}: {distance}")
