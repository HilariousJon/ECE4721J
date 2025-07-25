#!/usr/bin/env python3
import sys
import json
import numpy as np
import utils

INPUT_FEATURES_FILENAME = "input_song_features.json"
input_song_features = None


def setup():
    global input_song_features
    with open(INPUT_FEATURES_FILENAME, "r") as f:
        features_data = json.load(f)
    input_song_features = np.array(features_data, dtype=np.float64)


def main():
    setup()
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
    for line in sys.stdin:
        song_data = json.loads(line.strip())
        simple_features = [float(song_data.get(c, 0.0) or 0.0) for c in feature_cols]
        timbre_data = song_data.get("segments_timbre", [])
        candidate_features = np.array(simple_features + timbre_data, dtype=np.float64)
        candidate_meta = (
            song_data["title"],
            song_data["artist_name"],
            song_data["track_id"],
        )

        # Use the new cosine similarity function from utils.py
        score, _ = utils.calculate_distance(
            input_song_features, (candidate_features, candidate_meta)
        )

        print(f"1\t{score}\t{json.dumps(candidate_meta)}")


if __name__ == "__main__":
    main()
