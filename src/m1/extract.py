import os
import h5py
import sys
from fastavro import reader

def extract_hdf5_from_avro(avro_path, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    with open(avro_path, 'rb') as f:
        avro_reader = reader(f)

        for record in avro_reader:
            track_id = record["track_id"]
            analysis = record["analysis"]
            metadata = record["metadata"]

            out_path = os.path.join(output_dir, f"{track_id}.h5")

            with h5py.File(out_path, 'w') as h5f:
                grp_analysis = h5f.create_group("analysis")
                for key, val in analysis.items():
                    if isinstance(val, list):
                        grp_analysis.create_dataset(key, data=val)
                    else:
                        grp_analysis.attrs[key] = val

                grp_metadata = h5f.create_group("metadata")
                for key, val in metadata.items():
                    if isinstance(val, list):
                        grp_metadata.create_dataset(key, data=val)
                    elif val is not None:
                        grp_metadata.attrs[key] = val

            print(f"Reconstructed: {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python extract_hdf5_from_avro.py <avro_file> <output_dir>")
        sys.exit(1)

    avro_file = sys.argv[1]
    output_dir = sys.argv[2]
    extract_hdf5_from_avro(avro_file, output_dir)