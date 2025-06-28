from fastavro import reader
import h5py
import io

def extract_h5_from_avro(avro_file_path):
    with open(avro_file_path, 'rb') as f:
        avro_reader = reader(f)
        for record in avro_reader:
            filename = record["filename"]
            h5_bytes = record["h5data"]

            with h5py.File(io.BytesIO(h5_bytes), 'r') as h5f:
                print(f"\nContents of {filename}:")
                print(list(h5f.keys()))

if __name__ == "__main__":
    extract_h5_from_avro("your_file.avro")