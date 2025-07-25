#!/usr/bin/env python3
import sys
import json
import heapq

CANDIDATE_IDS_FILENAME = "candidate_song_ids.txt"
INPUT_SONG_ID_FILENAME = "input_song_id.txt"
TOP_N = 200

candidate_ids = {}
INPUT_SONG_ID = ""


def setup():
    global candidate_ids, INPUT_SONG_ID
    with open(CANDIDATE_IDS_FILENAME, "r") as f:
        for line in f:
            track_id = line.strip().split("\t")[0]
            candidate_ids[track_id] = True
    with open(INPUT_SONG_ID_FILENAME, "r") as f:
        INPUT_SONG_ID = f.read().strip()


def main():
    setup()
    local_top_songs = []
    for line in sys.stdin:
        song_data = json.loads(line)
        track_id = song_data.get("track_id")
        if candidate_ids.get(track_id) and track_id != INPUT_SONG_ID:
            hotttnesss = float(song_data.get("song_hotttnesss") or 0.0)
            heapq.heappush(local_top_songs, (hotttnesss, json.dumps(song_data)))
            if len(local_top_songs) > TOP_N:
                heapq.heappop(local_top_songs)
    for _, song_json in local_top_songs:
        print(f"1\t{song_json}")


if __name__ == "__main__":
    main()
