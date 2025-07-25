#!/usr/bin/env python3
import sys
import utils

META_DB_FILENAME = "track_metadata.db"


def main():
    for line in sys.stdin:
        artist_id, _ = line.strip().split("\t")
        songs = utils.get_songs_from_artist(artist_id, META_DB_FILENAME)
        print(f"Found {len(songs)} songs for artist ID {artist_id}")
        for song_title, track_id in songs:
            print(f"{track_id}\t{song_title}")


if __name__ == "__main__":
    main()
