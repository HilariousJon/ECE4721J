import argparse
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from graphframes import GraphFrame

def geo_dist(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2]:
        return float("inf")
    return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)

def get_neighbors(spark, dataPath, start_id, end_id):
    artist_df = spark.read.format("avro").load(dataPath)
    artist_df = artist_df.select(
        "artist_id", "artist_name", "artist_location",
        "artist_latitude", "artist_longitude",
        "artist_hotttnesss", "artist_familiarity"
    ).dropna(subset=["artist_id"])

    vertices_df = artist_df.select(col("artist_id").alias("id"))

    joined = artist_df.alias("a").join(
        artist_df.alias("b"),
        col("a.artist_id") < col("b.artist_id")
    )

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

    edges_df = edges.select(
        col("a.artist_id").alias("src"),
        col("b.artist_id").alias("dst")
    )

    graph = GraphFrame(vertices_df, edges_df)

    result = graph.bfs(
        fromExpr=f"id = '{start_id}'",
        toExpr=f"id = '{end_id}'",
        maxPathLength=5
    )

    paths = result.collect()
    if not paths:
        return -1

    distance = sum(1 for key in paths[0].asDict() if key.startswith("e"))
    return distance

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find artist distance using GraphFrame BFS")
    parser.add_argument("-d", "--data", required=True, help="Path to Avro file")
    parser.add_argument("-s", "--start", required=True, help="Start artist_id")
    parser.add_argument("-e", "--end", required=True, help="End artist_id")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("GetNeighbors").getOrCreate()
    
    distance = get_neighbors(spark, args.data, args.start, args.end)
    print(f"Distance from {args.start} to {args.end}: {distance}")
    
    spark.stop()