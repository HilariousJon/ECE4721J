import time
import json
import os
import numpy as np
from loguru import logger
from pathlib import Path
from fastavro import reader
from mrjob.job import MRJob
from mrjob.step import MRStep
from utils import (
    get_artist_neighbor,
    get_artist_from_song,
)
from src.m2.bfs.mapreduce_recommender import SongRecommenderMR


class MRJobWorkflow:
    def __init__(self, args_wrapper):
        (
            self.mode,
            self.artist_db_path,
            self.config,
            self.avro_path,
            self.song_id,
            self.meta_db_path,
            self.bfs_depth,
        ) = args_wrapper
        self.cache_path = self.avro_path.replace(".avro", ".json")

    def _run_bfs(self) -> list:
        logger.info("Workflow Step 1: Running local BFS to get artist list...")
        _, artist_id_list, artist_name_list = get_artist_from_song(
            self.song_id, self.meta_db_path
        )
        if not artist_name_list:
            logger.error(f"Artist name not found for song ID {self.song_id}. Exiting.")
            return []

        start_artist_id = artist_id_list[0]
        logger.info(
            f"Starting BFS from artist: {artist_name_list[0]} (ID: {start_artist_id})"
        )

        artists_frontier = {start_artist_id}
        all_artists = {start_artist_id}

        for i in range(self.bfs_depth):
            newly_found_artists = set()
            for artist_id in artists_frontier:
                neighbors = get_artist_neighbor(artist_id, self.artist_db_path)
                newly_found_artists.update(n for n in neighbors if n not in all_artists)

            if not newly_found_artists:
                logger.info(f"Depth {i + 1}: No new artists found. BFS finished early.")
                break

            artists_frontier = newly_found_artists
            all_artists.update(artists_frontier)
            logger.info(
                f"Depth {i + 1}: Found {len(newly_found_artists)} new artists. Total unique artists: {len(all_artists)}."
            )

        return list(all_artists)

    def _prepare_mrjob_inputs(self):
        logger.info("Workflow Step 2: Preparing inputs for MRJob...")

        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    logger.success(
                        f"Cache hit! Loading features from: {self.cache_path}"
                    )
                    full_track_id, input_features_json = json.load(f)
                    return full_track_id, input_features_json
            except (json.JSONDecodeError, IOError, TypeError) as e:
                logger.warning(
                    f"Cache file {self.cache_path} is invalid. Re-computing. Error: {e}"
                )

        logger.info(
            f"Cache miss. Computing features from source file: {self.avro_path}"
        )

        track_id_list, _, _ = get_artist_from_song(self.song_id, self.meta_db_path)
        if not track_id_list:
            raise ValueError(f"Could not find track info for song ID {self.song_id}")
        full_track_id = track_id_list[0]

        with open(self.avro_path, "rb") as fo:
            for record in reader(fo):
                if record["track_id"] == full_track_id:
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
                    ]
                    simple_features = [
                        float(record.get(c, 0.0) or 0.0) for c in feature_cols
                    ]
                    timbre_data = record.get("segments_timbre", [])

                    input_features = np.array(
                        simple_features + timbre_data, dtype=np.float64
                    )
                    input_features_json = json.dumps(input_features.tolist())

                    try:
                        result_to_cache = [full_track_id, input_features_json]
                        with open(self.cache_path, "w") as f:
                            json.dump(result_to_cache, f)
                        logger.info(f"Saved new features to cache: {self.cache_path}")
                    except IOError as e:
                        logger.error(
                            f"Failed to write to cache file {self.cache_path}: {e}"
                        )

                    return full_track_id, input_features_json

    def run(self):
        total_start_time = time.time()

        # perform bfs
        artist_ids = self._run_bfs()
        if not artist_ids:
            logger.warning("BFS did not find any artists. Exiting workflow.")
            return

        # prepare inputs for MRJob
        try:
            input_track_id, input_features_json = self._prepare_mrjob_inputs()
        except ValueError as e:
            logger.error(f"Failed to prepare MRJob inputs: {e}")
            return

        utils_path = Path(__file__).parent / "utils.py"
        # configure and run MRJob
        logger.info("Workflow Step 3: Configuring and launching MRJob...")
        mr_job = SongRecommenderMR(
            args=[
                "--file",
                str(utils_path),
                "--meta-db-path",
                self.meta_db_path,
                "--avro-path",
                self.avro_path,
                "--input-features-json",
                input_features_json,
                "--input-track-id",
                input_track_id,
            ]
            + ["-r", "hadoop"]
        )

        # giving the output to mrjob
        mr_job.sandbox(
            stdin=[f'"{artist_id}"\n'.encode("utf-8") for artist_id in artist_ids]
        )

        with mr_job.make_runner() as runner:
            runner.run()
            logger.info("Workflow Step 4: Processing final results...")
            final_result_found = False
            for key, value in mr_job.parse_output(runner.stdout):
                final_result_found = True
                similarity_score, (title, artist, tid) = key, value
                logger.success("Most similar song found:")
                logger.success(f"  Song name: {title}")
                logger.success(f"  Artist: {artist}")
                logger.success(f"  Track ID: {tid}")
                logger.success(f"  Similarity score: {similarity_score:.4f}")

            if not final_result_found:
                logger.warning("MRJob finished but produced no output.")

        logger.info(f"Total workflow finished in {time.time() - total_start_time:.2f}s")
