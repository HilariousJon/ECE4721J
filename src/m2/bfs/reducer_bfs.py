#!/usr/bin/env python3
import sys


def main():
    current_artist = None
    is_visited = False
    for line in sys.stdin:
        artist_id, node_type = line.strip().split("\t")
        if current_artist != artist_id:
            if current_artist and is_visited:
                print(f"{current_artist}\tV")
            current_artist = artist_id
            is_visited = False
        if node_type == "V":
            is_visited = True
    if current_artist and is_visited:
        print(f"{current_artist}\tV")


if __name__ == "__main__":
    main()
