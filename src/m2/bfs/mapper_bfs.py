import sys
import os

print("--- DEBUG: mapper_bfs.py starting ---", file=sys.stderr)
print(f"--- DEBUG: Python executable is {sys.executable}", file=sys.stderr)
print(f"--- DEBUG: Current Working Directory is {os.getcwd()}", file=sys.stderr)
print("--- DEBUG: Listing files in CWD:", file=sys.stderr)
try:
    files_in_dir = os.listdir(".")
    for f in files_in_dir:
        print(f"--- DEBUG: Found file: {f}", file=sys.stderr)
except Exception as e:
    print(f"--- DEBUG: Error listing files: {e}", file=sys.stderr)

try:
    print("--- DEBUG: Attempting to import utils ---", file=sys.stderr)
    import utils

    print("--- DEBUG: Successfully imported utils ---", file=sys.stderr)

    ARTIST_DB_FILENAME = "artist_similarity.db"
    print(
        f"--- DEBUG: Database filename to use is '{ARTIST_DB_FILENAME}' ---",
        file=sys.stderr,
    )

    def main():
        print("--- DEBUG: main() function started ---", file=sys.stderr)
        for line in sys.stdin:
            try:
                line = line.strip()
                print(f"--- DEBUG: Processing line: '{line}' ---", file=sys.stderr)
                artist_id, node_type = line.split("\t")
                if node_type == "V":
                    print(f"{artist_id}\tV")
                elif node_type == "F":
                    print(
                        f"--- DEBUG: Calling get_artist_neighbor for artist '{artist_id}' ---",
                        file=sys.stderr,
                    )
                    neighbors = utils.get_artist_neighbor(artist_id, ARTIST_DB_FILENAME)
                    print(
                        f"--- DEBUG: Found {len(neighbors)} neighbors for artist '{artist_id}' ---",
                        file=sys.stderr,
                    )
                    for neighbor_id in neighbors:
                        print(f"{neighbor_id}\tF")
                        print(f"{neighbor_id}\tV")
            except Exception as e:
                print(
                    f"FATAL: Error processing line '{line.strip()}'. Exception: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
        print("--- DEBUG: main() function finished successfully ---", file=sys.stderr)

    if __name__ == "__main__":
        main()

except Exception as e:
    print(
        f"FATAL: A critical error occurred before main loop. Exception: {e}",
        file=sys.stderr,
    )
    import traceback

    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
