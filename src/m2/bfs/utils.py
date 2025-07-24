import sqlite3
from typing import List, Tuple, Any
from numpy import ndarray
import numpy as np
from loguru import logger


def get_artist_neighbor(artist_id: str, db_path: str) -> List[str]:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # use parametrized query
        query = "SELECT similar FROM similarity WHERE target = ?"
        results = cursor.execute(query, (artist_id,)).fetchall()
        conn.close()
        return [col[0] for col in results]
    except sqlite3.Error as e:
        logger.error(f"DB error in get_artist_neighbor for artist {artist_id}: {e}")
        return []


def get_songs_from_artist(artist_id: str, db_path: str) -> List[Tuple[str, str, float]]:
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = "SELECT title, track_id, song_hotttnesss FROM songs WHERE artist_id = ?"
        results = cursor.execute(query, (artist_id,)).fetchall()
        conn.close()
        # Return a tuple including the hotness, handling None values.
        return [
            (col[0], col[1], col[2] if col[2] is not None else 0.0) for col in results
        ]
    except sqlite3.Error as e:
        logger.error(f"DB error in get_songs_from_artist for artist {artist_id}: {e}")
        return []


def get_artist_from_song(
    song_id: str, db_path: str
) -> Tuple[List[str], List[str], List[str]]:
    """Safely fetches artist info for a given song (track_id)."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = "SELECT track_id, artist_id, artist_name FROM songs WHERE track_id = ?"
        results = cursor.execute(query, (song_id,)).fetchall()
        conn.close()
        if not results:
            return [], [], []
        return (
            [col[0] for col in results],
            [col[1] for col in results],
            [col[2] for col in results],
        )
    except sqlite3.Error as e:
        logger.error(f"DB error in get_artist_from_song for song {song_id}: {e}")
        return [], [], []


def merge_string_lists(list1: List[str], list2: List[str]) -> List[str]:
    return list(set(list1) | set(list2))


def merge_song_tuples(list1: List[Tuple], list2: List[Tuple]) -> List[Tuple]:
    seen = set()
    result = []
    for item in list1 + list2:
        identifier = item[1]
        if identifier not in seen:
            seen.add(identifier)
            result.append(item)
    return result


def calculate_distance(
    song1_features: ndarray, song2_data: Tuple[ndarray, tuple]
) -> Tuple[float, tuple]:
    song2_features, song2_metadata = song2_data
    norm1 = np.linalg.norm(song1_features)
    norm2 = np.linalg.norm(song2_features)
    if norm1 == 0 or norm2 == 0:
        return (-np.inf, song2_metadata)
    cosine_similarity = np.dot(song1_features, song2_features) / (norm1 * norm2)
    # l1_distance = np.sum(np.abs(song1_features - song2_features))
    # score = cosine_similarity - l1_distance
    score = cosine_similarity
    return (score, song2_metadata)
