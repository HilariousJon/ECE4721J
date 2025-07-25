from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from typing import List, Tuple, Any
from src.m2.bfs.utils import (
    get_artist_from_song,
    get_artist_neighbor,
    get_songs_from_artist,
    merge_lists,
    calculate_distance,
)
from loguru import logger
import time
import os
import numpy as np
import heapq


def run_bfs_spark(args_wrapper: Tuple[str, str, str, str, str, str, int]) -> None:
    (_, artist_db_path, _, avro_path, song_id, meta_db_path, bfs_depth) = args_wrapper

    spark = (
        SparkSession.builder.appName("BFS Artist Similarity")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.4")
        .getOrCreate()
    )

    sc: SparkContext = spark.sparkContext

    try:
        local_avro_path = f"file://{os.path.abspath(avro_path)}"
        logger.info(f"Loading avro data from local path: {local_avro_path}...")
        song_df = spark.read.format("avro").load(local_avro_path)

        track_id_list, artist_id_list, artist_name_list = get_artist_from_song(
            song_id, meta_db_path
        )

        if not artist_name_list:
            logger.error(f"Artist name not found for song ID {song_id}. Exiting.")
            return

        artist_name = artist_name_list[0]
        artist_id = artist_id_list[0]
        track_id = track_id_list[0]
        artists = [artist_id]

        logger.info(
            f"Starting BFS from artist: {artist_name} (ID: {artist_id}) for song: {track_id}"
        )

        start_time = time.time()
        for i in range(bfs_depth):
            newly_found_artists = (
                sc.parallelize(artists, 8)
                .map(lambda x: get_artist_neighbor(x, artist_db_path))
                .reduce(merge_lists)
            )
            artists.extend(newly_found_artists)
            logger.info(
                f"Depth {i + 1}: Artist list size is now {len(artists)} (including duplicates from previous layers)."
            )

        logger.info(f"BFS finished. Total artist entries found: {len(artists)}.")
        unique_artists = list(set(artists))
        logger.info(f"Deduplicated to {len(unique_artists)} unique artists.")

        songs_tuples: List[Tuple[str, str]] = (
            sc.parallelize(unique_artists, 16)
            .map(lambda x: get_songs_from_artist(x, meta_db_path))
            .reduce(merge_lists)
        )

        songs_tuples = [s for s in songs_tuples if s[1] != track_id]
        candidate_songs_ids = [tup[1] for tup in songs_tuples]

        logger.info(
            f"BFS finished in {time.time() - start_time:.2f}s. Found {len(candidate_songs_ids)} candidate songs."
        )

        if not candidate_songs_ids:
            logger.warning("No candidate songs found after BFS. Exiting.")
            return

        logger.info(
            f"Pre-filtering {len(candidate_songs_ids)} candidates using MapReduce-style Top-N logic..."
        )

        candidate_ids_df = spark.createDataFrame(
            [(id,) for id in candidate_songs_ids], ["track_id"]
        )

        candidate_songs_df = song_df.join(broadcast(candidate_ids_df), "track_id")

        def get_partition_top_n(iterator: iter, n: int = 200) -> iter:
            local_top_songs = []
            for row in iterator:
                hotttnesss = (
                    row["song_hotttnesss"]
                    if row["song_hotttnesss"] is not None
                    else float("-inf")
                )

                if len(local_top_songs) < n:
                    heapq.heappush(local_top_songs, (hotttnesss, row))
                else:
                    heapq.heappushpop(local_top_songs, (hotttnesss, row))

            return iter([row for score, row in local_top_songs])

        local_top_rdd = candidate_songs_df.rdd.mapPartitions(get_partition_top_n)

        all_local_tops = local_top_rdd.collect()

        global_top_200_rows = sorted(
            all_local_tops,
            key=lambda row: (
                row["song_hotttnesss"]
                if row["song_hotttnesss"] is not None
                else float("-inf")
            ),
            reverse=True,
        )[:200]

        if not global_top_200_rows:
            logger.warning("No candidate songs left after Top-N filtering. Exiting.")
            return

        hottest_candidates_df = spark.createDataFrame(global_top_200_rows)

        logger.info(
            f"Pre-filtering {len(candidate_songs_ids)} candidates by song_hotttnesss, taking top 200..."
        )

        feature_cols = [
            "loudness",
            "tempo",
            "duration",
            "energy",
            "danceability",
            "key",
            "mode",
            "time_signature",
            "song_hotttnesss",
            # "artist_hotttnesss",
            # "artist_familiarity",
        ]
        metadata_cols = ["title", "artist_name", "track_id"]

        # Use the pre-filtered DataFrame for the final comparison
        candidate_features_df = hottest_candidates_df.select(
            feature_cols + metadata_cols + ["segments_timbre"]
        )

        input_song_row = (
            song_df.filter(col("track_id") == track_id)
            .select(feature_cols + ["segments_timbre"])
            .first()
        )
        simple_features = [
            float(v) if v is not None else 0.0
            for v in input_song_row[: len(feature_cols)]
        ]

        timbre_data = input_song_row["segments_timbre"]

        input_song_features = np.array(
            simple_features + timbre_data,
            dtype=np.float64,
        )

        broadcast_input_features = sc.broadcast(input_song_features)

        logger.info("Calculating similarity scores for the top hottest songs...")
        features_rdd = candidate_features_df.rdd.map(
            lambda row: (
                np.array(
                    [float(row[c]) if row[c] is not None else 0.0 for c in feature_cols]
                    + [
                        timbre for timbre in row["segments_timbre"]
                    ],  # deal with timbre segments
                    dtype=np.float64,
                ),
                (row["title"], row["artist_name"], row["track_id"]),
            )
        )

        if features_rdd.isEmpty():
            logger.warning(
                "No candidate songs with features found to compare against. Exiting."
            )
            return

        most_similar_song = features_rdd.map(
            lambda candidate_data: calculate_distance(
                broadcast_input_features.value, candidate_data
            )
        ).reduce(max)

        similarity_score, (title, artist, tid) = most_similar_song

        logger.success("Most similar song found:")
        logger.success(f"  Song name: {title}")
        logger.success(f"  Artist: {artist}")
        logger.success(f"  Track ID: {tid}")
        logger.success(f"  Similarity score: {similarity_score:.4f}")

    finally:
        logger.info("Closing Spark Session...")
        spark.stop()
