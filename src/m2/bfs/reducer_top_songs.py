#!/usr/bin/env python3
import sys
import json
import heapq

TOP_N = 200


def main():
    global_top_songs = []
    for line in sys.stdin:
        _, song_json = line.strip().split("\t", 1)
        song_data = json.loads(song_json)
        hotttnesss = float(song_data.get("song_hotttnesss") or 0.0)
        heapq.heappush(global_top_songs, (hotttnesss, song_json))
        if len(global_top_songs) > TOP_N:
            heapq.heappop(global_top_songs)
    for _, song_json in sorted(global_top_songs, key=lambda x: x[0], reverse=True):
        print(song_json)


if __name__ == "__main__":
    main()
