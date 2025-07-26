import argparse
import time
from collections import deque, defaultdict

import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, DoubleType
from sklearn.metrics.pairwise import cosine_similarity

def load_graph(spark, path):
    df = spark.read.parquet(path)
    return {row["song_id"]: set(row["neighbors"]) for row in df.collect()}

def gen_bfs_score(graph, seeds, max_depth=3):
    score_map = defaultdict(float)
    for seed in seeds:
        visited = set()
        queue = deque([(seed, 0)])
        while queue:
            current, depth = queue.popleft()
            if current in visited or depth > max_depth:
                continue
            visited.add(current)
            if depth > 0:
                score_map[current] += 1.0 / depth
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    queue.append((neighbor, depth + 1))
    return score_map

def recommend(graph_path, feature_path, seed_ids, top_k=10, w_sim=0.5, w_hot=0.3, w_bfs=0.2):
    spark = SparkSession.builder.appName("SongGraphRecommendation").getOrCreate()
    start_time = time.time()

    # Load graph
    graph = load_graph(spark, graph_path)
    print("Performing BFS scoring...")
    bfs_scores = gen_bfs_score(graph, seed_ids)
    candidate_ids = list(bfs_scores.keys())
    if not candidate_ids:
        print("No candidate nodes found in 3-hop BFS.")
        return

    # Load feature data
    print("Loading feature data...")
    raw_df = spark.read.format("avro").load(feature_path)

    # Flatten timbre
    def flatten_timbre(arr):
        if arr is None:
            return [0.0] * 90
        return (arr + [0.0] * 90)[:90]

    flatten_udf = udf(flatten_timbre, ArrayType(DoubleType()))
    df = raw_df.withColumn("timbre", flatten_udf(col("segments_timbre")))

    for i in range(90):
        df = df.withColumn(f"timbre_{i}", col("timbre").getItem(i))

    # Define feature columns
    feature_cols = [
        "year", "duration", "loudness", "tempo", "energy", "danceability",
        "key", "mode", "time_signature", "song_hotttnesss"
    ] + [f"timbre_{i}" for i in range(12)]

    all_timbre_cols = [f"timbre_{i}" for i in range(90)]

    df = df.select("artist_name", "song_id", "title", *feature_cols, *all_timbre_cols).dropna()

    # Extract seed features
    seed_features = df.filter(col("song_id").isin(seed_ids)).collect()
    if not seed_features:
        print("No seed features found.")
        return

    seed_matrix = np.array([[row[c] for c in feature_cols] for row in seed_features])
    seed_vector = np.mean(seed_matrix, axis=0).reshape(1, -1)

    candidate_df = df.filter(col("song_id").isin(candidate_ids))
    candidate_rows = candidate_df.collect()

    similarities = []
    hotttnessses = []
    candidate_info = {}

    for row in candidate_rows:
        song_id = row["song_id"]
        vector = np.array([row[c] for c in feature_cols]).reshape(1, -1)
        similarity = float(cosine_similarity(seed_vector, vector)[0][0])
        hotttness = float(row["song_hotttnesss"])
        bfs_val = bfs_scores.get(song_id, 0.0)

        similarities.append((song_id, similarity))
        hotttnessses.append((song_id, hotttness))
        candidate_info[song_id] = {
            "title": row["title"],
            "artist_name": row["artist_name"],
            "similarity": similarity,
            "hotttnesss": hotttness,
            "bfs_score": bfs_val
        }

    sim_values = [s for _, s in similarities]
    hot_values = [h for _, h in hotttnessses]

    sim_min, sim_max = min(sim_values), max(sim_values)
    hot_min, hot_max = min(hot_values), max(hot_values)

    sim_range = sim_max - sim_min if sim_max > sim_min else 1e-6
    hot_range = hot_max - hot_min if hot_max > hot_min else 1e-6

    # Normalize weights
    weight_sum = w_sim + w_hot + w_bfs
    w_sim /= weight_sum
    w_hot /= weight_sum
    w_bfs /= weight_sum

    scored_candidates = []
    for song_id in candidate_info:
        sim_norm = (candidate_info[song_id]["similarity"] - sim_min) / sim_range
        hot_norm = (candidate_info[song_id]["hotttnesss"] - hot_min) / hot_range
        bfs_score = candidate_info[song_id]["bfs_score"]

        final_score = sim_norm * w_sim + hot_norm * w_hot + bfs_score * w_bfs
        scored_candidates.append((
            song_id,
            candidate_info[song_id]["title"],
            candidate_info[song_id]["artist_name"],
            final_score
        ))

    top_recommendations = sorted(scored_candidates, key=lambda x: -x[3])[:top_k]

    print("\nInput Songs:")
    input_df = df.filter(col("song_id").isin(seed_ids)) \
      .select("song_id", "title", "artist_name") \
      .distinct()
    input_df.show(truncate=False)

    print("\nTop Recommendations:")
    rec_df = spark.createDataFrame([
        {"song_id": sid, "title": title, "artist_name": artist, "score": score}
        for sid, title, artist, score in top_recommendations
    ])
    rec_df.select("song_id", "title", "artist_name", "score").show(truncate=False)

    # Compute and display similarity matrix (90-dim timbre)
    print("\nSimilarity Matrix (90-dim Timbre Features):")
    seed_vecs90 = df.filter(col("song_id").isin(seed_ids)) \
                  .select("song_id", *all_timbre_cols).rdd \
                  .map(lambda row: (row["song_id"], np.array([row[f"timbre_{i}"] for i in range(90)]))) \
                  .collect()

    rec_vecs90 = df.filter(col("song_id").isin([sid for sid, _, _, _ in top_recommendations])) \
                  .select("song_id", *all_timbre_cols).rdd \
                  .map(lambda row: (row["song_id"], np.array([row[f"timbre_{i}"] for i in range(90)]))) \
                  .collect()

    # Build similarity matrix using 90-dim timbre vectors
    import pandas as pd
    
    sim_matrix = pd.DataFrame(
        index=[rec_id for rec_id, _ in rec_vecs90],      # rows: topk songs
        columns=[seed_id for seed_id, _ in seed_vecs90]  # cols: input songs
    )

    for rec_id, rec_vec in rec_vecs90:
        for seed_id, seed_vec in seed_vecs90:
            sim = float(cosine_similarity(seed_vec.reshape(1, -1), rec_vec.reshape(1, -1))[0][0])
            sim_matrix.at[rec_id, seed_id] = sim

    print("\nSimilarity Matrix (rows: topK songs, cols: input seeds):")
    print(sim_matrix.to_string())

    spark.stop()
    print(f"Total time: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Graph-based Song Recommendation")
    parser.add_argument("-g", "--graph", required=True, help="Path to graph parquet file")
    parser.add_argument("-f", "--features", required=True, help="Path to song features avro file")
    parser.add_argument("-s", "--seeds", required=True, nargs="+", help="List of seed song IDs")
    parser.add_argument("-k", "--topk", type=int, default=10, help="Number of recommendations")
    parser.add_argument("--w_sim", type=float, default=0.5, help="Weight for cosine similarity")
    parser.add_argument("--w_hot", type=float, default=0.3, help="Weight for song hotttnesss")
    parser.add_argument("--w_bfs", type=float, default=0.2, help="Weight for BFS score")
    args = parser.parse_args()

    recommend(
        args.graph,
        args.features,
        args.seeds,
        args.topk,
        args.w_sim,
        args.w_hot,
        args.w_bfs
    )