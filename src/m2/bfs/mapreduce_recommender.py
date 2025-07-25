import json
import numpy as np
from loguru import logger
from fastavro import reader
from mrjob.job import MRJob
from mrjob.step import MRStep
from utils import (
    get_songs_from_artist,
    calculate_distance,
)


class SongRecommenderMR(MRJob):

    def configure_args(self):
        super(SongRecommenderMR, self).configure_args()
        # Path to the metadata SQLite DB on the cluster nodes
        self.add_file_arg("--meta-db-path")
        # Path to the Avro data file on the cluster nodes
        self.add_file_arg("--avro-path")
        # The input song's complete feature vector, passed as a JSON string
        self.add_passthru_arg("--input-features-json", type=str)
        # The track ID of the input song, to avoid comparing it with itself
        self.add_passthru_arg("--input-track-id", type=str)

    def load_args(self, args):
        super(SongRecommenderMR, self).load_args(args)

    def mapper_init_get_songs(self):
        self.song_features_map = {}
        # self.options.avro_path now refers to the local path on the task node
        with open(self.options.avro_path, "rb") as fo:
            for record in reader(fo):
                self.song_features_map[record["track_id"]] = record

    def mapper_get_songs(self, _, artist_id):
        # self.options.meta_db_path is the path to the DB file on the node
        songs = get_songs_from_artist(artist_id, self.options.meta_db_path)
        for title, track_id in songs:
            # Exclude the original input song
            if track_id == self.options.input_track_id:
                continue

            song_info = self.song_features_map.get(track_id)
            if song_info and song_info.get("song_hotttnesss") is not None:
                hotttnesss = song_info["song_hotttnesss"]
                artist_name = song_info.get("artist_name", "Unknown Artist")
                # Yield a constant key to send all songs to a single reducer for Top-N
                yield 1, (hotttnesss, track_id, title, artist_name)

    def reducer_top_n_songs(self, key, values):
        sorted_songs = sorted(list(values), key=lambda x: x[0], reverse=True)
        for song_data in sorted_songs[:200]:
            yield None, song_data

    def mapper_init_calc_similarity(self):
        # Load the Avro data map again for this step's mappers
        self.song_features_map = {}
        with open(self.options.avro_path, "rb") as fo:
            for record in reader(fo):
                self.song_features_map[record["track_id"]] = record

        # Parse the input song's features from the JSON string argument
        self.input_song_features = np.array(
            json.loads(self.options.input_features_json), dtype=np.float64
        )

        # Define feature columns for vector construction
        self.feature_cols = [
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

    def mapper_calc_similarity(self, _, song_data):
        _, track_id, title, artist_name = song_data
        candidate_song_info = self.song_features_map.get(track_id)

        if candidate_song_info:
            simple_features = [
                float(candidate_song_info.get(c, 0.0) or 0.0) for c in self.feature_cols
            ]
            timbre_data = candidate_song_info.get("segments_timbre", [])
            candidate_features = np.array(
                simple_features + timbre_data, dtype=np.float64
            )

            metadata = (title, artist_name, track_id)
            score, _ = calculate_distance(
                self.input_song_features, (candidate_features, metadata)
            )

            # Yield a constant key to find the global maximum in a single reducer
            yield 1, (score, metadata)

    def reducer_find_max_similarity(self, key, values):
        yield max(list(values), key=lambda x: x[0])

    def steps(self):
        # Define the steps for the MRJob workflow
        return [
            MRStep(
                mapper_init=self.mapper_init_get_songs,
                mapper=self.mapper_get_songs,
                reducer=self.reducer_top_n_songs,
            ),
            MRStep(
                mapper_init=self.mapper_init_calc_similarity,
                mapper=self.mapper_calc_similarity,
                reducer=self.reducer_find_max_similarity,
            ),
        ]


if __name__ == "__main__":
    SongRecommenderMR.run()
