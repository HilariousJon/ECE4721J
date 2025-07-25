import sys
import utils


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python3 1_run_setup.py <track_id> <meta_db_path>", file=sys.stderr
        )
        sys.exit(1)

    track_id = sys.argv[1]
    meta_db_path = sys.argv[2]

    (
        _,
        artist_id_list,
        _,
    ) = utils.get_artist_from_song(track_id, meta_db_path)
    if not artist_id_list:
        print(f"Error: Artist not found for track {track_id}", file=sys.stderr)
        exit(1)

    initial_artist_id = artist_id_list[0]

    with open("initial_artists.txt", "w") as f:
        f.write(f"{initial_artist_id}\tF\n")
        f.write(f"{initial_artist_id}\tV\n")

    print(f"Initial setup complete. Start artist: {initial_artist_id}")
    print("Created 'initial_artists.txt'.")
    print(
        "IMPORTANT: Ensure 'input_song_features.json' and 'song_data.jsonl' are ready."
    )


if __name__ == "__main__":
    main()
