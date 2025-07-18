import argparse
import json
from fastavro import reader

def convert_avro_to_json(input_avro_path, output_json_path):
    with open(input_avro_path, 'rb') as avro_file, open(output_json_path, 'w') as out_file:
        for record in reader(avro_file):
            if all(record.get(f) is not None for f in ["artist_id", "artist_hotttnesss", "artist_familiarity", "artist_latitude", "artist_longitude"]):
                filtered = {
                    "artist_id": record["artist_id"],
                    "hotttnesss": record["artist_hotttnesss"],
                    "familiarity": record["artist_familiarity"],
                    "lat": record["artist_latitude"],
                    "lon": record["artist_longitude"]
                }
                out_file.write(json.dumps(filtered) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    convert_avro_to_json(args.input, args.output)
    print(f"Converted {args.input} to {args.output}")