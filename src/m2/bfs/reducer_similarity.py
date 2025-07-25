#!/usr/bin/env python3
import sys
import numpy as np
import json


def main():
    # Use a very small number for initialization since scores can be negative.
    max_score = -np.inf
    best_song_meta = None
    for line in sys.stdin:
        _, score_str, meta_json = line.strip().split("\t", 2)
        score = float(score_str)
        if score > max_score:
            max_score = score
            best_song_meta = json.loads(meta_json)
    if best_song_meta:
        title, artist, tid = best_song_meta
        print(f"Most similar song found:")
        print(f"  Song name: {title}")
        print(f"  Artist: {artist}")
        print(f"  Track ID: {tid}")
        print(f"  Similarity score: {max_score:.4f}")


if __name__ == "__main__":
    main()
