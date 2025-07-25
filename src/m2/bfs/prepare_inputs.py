import argparse
import json
import os
import sys
import numpy as np
from loguru import logger
from fastavro import reader

sys.path.insert(0, os.path.abspath("."))
from src.m2.bfs.utils import get_artist_from_song, get_artist_neighbor


def run_bfs(song_id, meta_db_path, artist_db_path, bfs_depth) -> list:
    logger.info("Running local BFS to get artist list...")
    _, artist_id_list, artist_name_list = get_artist_from_song(song_id, meta_db_path)
    if not artist_name_list:
        logger.error(f"Artist not found for song ID {song_id}.")
        return []

    start_artist_id = artist_id_list[0]
    logger.info(
        f"Starting BFS from artist: {artist_name_list[0]} (ID: {start_artist_id})"
    )

    artists_frontier = {start_artist_id}
    all_artists = {start_artist_id}

    for i in range(bfs_depth):
        newly_found = set()
        for artist_id in artists_frontier:
            neighbors = get_artist_neighbor(artist_id, artist_db_path)
            newly_found.update(n for n in neighbors if n not in all_artists)

        if not newly_found:
            logger.info(f"Depth {i + 1}: No new artists found. BFS finished early.")
            break

        artists_frontier = newly_found
        all_artists.update(artists_frontier)
        logger.info(f"Depth {i + 1}: Total unique artists found: {len(all_artists)}.")

    return list(all_artists)


def extract_features(song_id, meta_db_path, avro_path, cache_dir):
    logger.info("Extracting features for input song...")

    # 设置缓存
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f"{song_id}.feature.json")

    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r") as f:
                logger.success(f"Cache hit! Loading features from: {cache_path}")
                return json.load(f)
        except Exception as e:
            logger.warning(f"Cache file invalid, re-computing. Error: {e}")

    logger.info(f"Cache miss. Computing features from source: {avro_path}")
    track_id_list, _, _ = get_artist_from_song(song_id, meta_db_path)
    if not track_id_list:
        raise ValueError(f"Could not find track info for song ID {song_id}")
    full_track_id = track_id_list[0]

    with open(avro_path, "rb") as fo:
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

                features_list = np.array(simple_features + timbre_data).tolist()
                result = {
                    "track_id": full_track_id,
                    "features_json": json.dumps(features_list),
                }

                with open(cache_path, "w") as f:
                    json.dump(result, f)
                logger.info(f"Saved features to cache: {cache_path}")
                return result

    raise ValueError(f"Could not find song features for track ID {full_track_id}")


def main():
    parser = argparse.ArgumentParser(description="Prepare inputs for Hadoop workflow.")
    parser.add_argument("--song-id", required=True)
    parser.add_argument("--bfs-depth", required=True, type=int)
    parser.add_argument("--meta-db", required=True)
    parser.add_argument("--artist-db", required=True)
    parser.add_argument("--avro-path", required=True)
    parser.add_argument(
        "--out-artists", required=True, help="Output file for artist list"
    )
    parser.add_argument(
        "--out-features", required=True, help="Output file for song features"
    )
    parser.add_argument(
        "--cache-dir", default="./.cache", help="Directory for feature cache"
    )
    args = parser.parse_args()

    artists = run_bfs(args.song_id, args.meta_db, args.artist_db, args.bfs_depth)
    with open(args.out_artists, "w") as f:
        for artist_id in artists:
            f.write(f'"{artist_id}"\n')
    logger.success(f"Artist list saved to {args.out_artists}")

    features = extract_features(
        args.song_id, args.meta_db, args.avro_path, args.cache_dir
    )
    with open(args.out_features, "w") as f:
        json.dump(features, f)
    logger.success(f"Input song features saved to {args.out_features}")


if __name__ == "__main__":
    main()
