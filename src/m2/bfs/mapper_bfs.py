#!/usr/bin/env python3
import sys
import utils

ARTIST_DB_FILENAME = "./data/artist_similarity.db"


def main():
    for line in sys.stdin:
        artist_id, node_type = line.strip().split("\t")
        if node_type == "V":
            print(f"{artist_id}\tV")
        elif node_type == "F":
            neighbors = utils.get_artist_neighbor(artist_id, ARTIST_DB_FILENAME)
            for neighbor_id in neighbors:
                print(f"{neighbor_id}\tF")
                print(f"{neighbor_id}\tV")


if __name__ == "__main__":
    main()
