import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from collections import deque
import time

def load_graph(spark, path):
    df = spark.read.parquet(path)
    return {row["artist_id"]: set(row["neighbors"]) for row in df.collect()}

def bfs_shortest_path(graph, start_id, end_id, max_depth=10):
    if start_id not in graph or end_id not in graph:
        return -1
    visited = set()
    queue = deque([(start_id, 0)])

    while queue:
        current, depth = queue.popleft()
        if current == end_id:
            return depth
        if depth >= max_depth:
            continue
        visited.add(current)
        for neighbor in graph.get(current, []):
            if neighbor not in visited:
                queue.append((neighbor, depth + 1))

    return -1

def query_distance(graph_path, start_id, end_id):
    spark = SparkSession.builder.appName("ArtistDistanceQuery").getOrCreate()
    graph = load_graph(spark, graph_path)
    start_time = time.time()
    distance = bfs_shortest_path(graph, start_id, end_id)
    if distance == -1:
        print(f"No path found between {start_id} and {end_id}")
    else:
        print(f"Shortest path from {start_id} to {end_id}: {distance} hop(s)")
    end_time = time.time()
    spark.stop()
    print(f"Query time: {end_time - start_time} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query artist distance in graph")
    parser.add_argument("-g", "--graph", required=True, help="Path to graph JSON file")
    parser.add_argument("-s", "--start", required=True, help="Start artist ID")
    parser.add_argument("-e", "--end", required=True, help="End artist ID")
    args = parser.parse_args()

    query_distance(args.graph, args.start, args.end)
