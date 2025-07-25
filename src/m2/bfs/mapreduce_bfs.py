import sys
import json
import numpy as np
import os
import argparse
from fastavro import reader

from utils import get_songs_from_artist, calculate_distance

META_DB_PATH = os.environ.get("META_DB_PATH")
AVRO_PATH = os.environ.get("AVRO_PATH")
INPUT_TRACK_ID = os.environ.get("INPUT_TRACK_ID")
INPUT_FEATURES_JSON = os.environ.get("INPUT_FEATURES_JSON")


def load_song_features_map(avro_path):
    song_map = {}
    with open(avro_path, "rb") as fo:
        for record in reader(fo):
            song_map[record["track_id"]] = record
    return song_map


def mapper_get_songs():
    song_features_map = load_song_features_map(AVRO_PATH)
    for line in sys.stdin:
        artist_id = line.strip().strip('"')
        if not artist_id:
            continue

        songs = get_songs_from_artist(artist_id, META_DB_PATH)
        for title, track_id in songs:
            if track_id == INPUT_TRACK_ID:
                continue

            song_info = song_features_map.get(track_id)
            if song_info and song_info.get("song_hotttnesss") is not None:
                hotttnesss = song_info["song_hotttnesss"]
                artist_name = song_info.get("artist_name", "Unknown Artist")
                print(f"1\t{json.dumps((hotttnesss, track_id, title, artist_name))}")


def reducer_top_n_songs():
    all_songs = []
    for line in sys.stdin:
        _key, value_str = line.strip().split("\t", 1)
        all_songs.append(json.loads(value_str))

    sorted_songs = sorted(all_songs, key=lambda x: x[0], reverse=True)

    for song_data in sorted_songs[:200]:
        print(json.dumps(song_data))


def mapper_calc_similarity():
    song_features_map = load_song_features_map(AVRO_PATH)
    input_song_features = np.array(json.loads(INPUT_FEATURES_JSON), dtype=np.float64)
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
        _hotttnesss, track_id, title, artist_name = song_data

        candidate_song_info = song_features_map.get(track_id)
        if candidate_song_info:
            simple_features = [
                float(candidate_song_info.get(c, 0.0) or 0.0) for c in feature_cols
            ]
            timbre_data = candidate_song_info.get("segments_timbre", [])
            candidate_features = np.array(
                simple_features + timbre_data, dtype=np.float64
            )

            metadata = (title, artist_name, track_id)
            score, _ = calculate_distance(
                input_song_features, (candidate_features, metadata)
            )

            print(f"1\t{json.dumps((score, metadata))}")


def reducer_find_max_similarity():
    max_song = (-float("inf"), None)
    for line in sys.stdin:
        _key, value_str = line.strip().split("\t", 1)
        current_song = json.loads(value_str)
        if current_song[0] > max_song[0]:
            max_song = current_song

    if max_song[1]:
        score, (title, artist, tid) = max_song
        print(f"{score}\t{title}\t{artist}\t{tid}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", required=True)
    args = parser.parse_args()

    if args.phase == "mapper1":
        mapper_get_songs()
    elif args.phase == "reducer1":
        reducer_top_n_songs()
    elif args.phase == "mapper2":
        mapper_calc_similarity()
    elif args.phase == "reducer2":
        reducer_find_max_similarity()
