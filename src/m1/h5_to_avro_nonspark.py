#!/usr/bin/env python3
import fastavro
import os
import numpy as np
import sys
import argparse
from typing import List, Dict, Any, Tuple, Iterator
from tqdm import tqdm
from loguru import logger
import hdf5_getters
from concurrent.futures import ThreadPoolExecutor, as_completed

# Logger setup remains the same
logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>",
)


def parse_args() -> Tuple[str, str, str, int]:
    parser = argparse.ArgumentParser(
        description="Convert MSD HDF5 to Avro using fastavro (Low Memory Mode)"
    )
    parser.add_argument(
        "-i", "--input", required=True, dest="hdf5_dir", help="HDF5 root directory"
    )
    parser.add_argument(
        "-o", "--output", required=True, dest="avro_dir", help="Avro output directory"
    )
    parser.add_argument(
        "-s", "--schema", required=True, dest="schema_path", help="Avro schema file"
    )
    parser.add_argument(
        "-t", "--threads", type=int, default=0, dest="threads", help="Number of threads"
    )
    args = parser.parse_args()
    if not os.path.isdir(args.hdf5_dir):
        logger.error(f"Input directory does not exist: {args.hdf5_dir}")
        sys.exit(1)
    if not os.path.isfile(args.schema_path):
        logger.error(f"Schema file does not exist: {args.schema_path}")
        sys.exit(1)
    os.makedirs(args.avro_dir, exist_ok=True)
    logger.info(f"Schema   : {args.schema_path}")
    logger.info(f"Input    : {args.hdf5_dir}")
    logger.info(f"Output   : {args.avro_dir}")
    if args.threads > 0:
        logger.info(f"Threads  : {args.threads}")
    return args.schema_path, args.hdf5_dir, args.avro_dir, args.threads


def get_field_type(field_schema: Dict[str, Any]) -> str | None:
    type_info = field_schema.get("type")
    if isinstance(type_info, list):
        base_type = next((t for t in type_info if t != "null"), None)
    else:
        base_type = type_info
    if isinstance(base_type, dict):
        base_type = base_type.get("type")
    if not isinstance(base_type, str):
        return None
    t = base_type.lower()
    if t == "string":
        return "str"
    if t in ("int", "long"):
        return "int"
    if t in ("float", "double"):
        return "float"
    if t == "array":
        return "array"
    return None


def extract_hdf5_data(h5_path: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    h5 = hdf5_getters.open_h5_file_read(h5_path)
    record: Dict[str, Any] = {}
    try:
        for field in schema["fields"]:
            getter_name = "get_" + field["name"]
            if not hasattr(hdf5_getters, getter_name):
                continue
            try:
                raw_value = getattr(hdf5_getters, getter_name)(h5)
                ftype = get_field_type(field)
                if ftype == "str":
                    record[field["name"]] = (
                        raw_value.decode("utf-8")
                        if isinstance(raw_value, bytes)
                        else str(raw_value)
                    )
                elif ftype == "int":
                    record[field["name"]] = int(raw_value)
                elif ftype == "float":
                    record[field["name"]] = float(raw_value)
                elif ftype == "array":
                    if raw_value is not None:
                        safe_array = np.nan_to_num(np.array(raw_value))
                        record[field["name"]] = safe_array.tolist()
                    else:
                        record[field["name"]] = None
                else:
                    record[field["name"]] = raw_value
            except Exception as e:
                logger.error(f"Error reading {field['name']} from {h5_path}: {e}")
    finally:
        h5.close()
    return record


def find_all_h5_files(root: str) -> List[str]:
    paths: List[str] = []
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if fn.endswith((".h5", ".hdf5")):
                paths.append(os.path.join(dirpath, fn))
    return paths


def generate_records(
    h5_files: List[str], schema: Dict[str, Any]
) -> Iterator[Dict[str, Any]]:
    for h5_path in h5_files:
        rec = extract_hdf5_data(h5_path, schema)
        if rec:
            yield rec


def aggregate_letter(
    schema_path: str, hdf5_root: str, avro_dir: str, letter: str
) -> None:
    parsed_schema = fastavro.schema.load_schema(schema_path)
    subfolder = os.path.join(hdf5_root, letter)
    output_file = os.path.join(avro_dir, f"{letter}.avro")
    h5_files = find_all_h5_files(subfolder)

    if not h5_files:
        return

    # Create a generator that will provide records one by one
    records_generator = generate_records(h5_files, parsed_schema)

    try:
        with open(output_file, "wb") as out_fp:
            # fastavro.writer can consume a generator directly, writing record by record
            fastavro.writer(out_fp, parsed_schema, records_generator)
        logger.info(f"[{letter}] Successfully wrote records to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write Avro file for letter '{letter}': {e}")


def main():
    schema_path, hdf5_root, avro_dir, num_threads = parse_args()
    logger.info("Starting conversion with fastavro (Low Memory Mode)")

    letters = [
        d for d in os.listdir(hdf5_root) if os.path.isdir(os.path.join(hdf5_root, d))
    ]

    if num_threads and num_threads > 1:
        logger.info(f"Processing with {num_threads} threads...")
        # sequentially to keep memory low within the thread.
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = {
                executor.submit(
                    aggregate_letter, schema_path, hdf5_root, avro_dir, letter
                ): letter
                for letter in letters
            }
            for future in tqdm(
                as_completed(futures), total=len(futures), desc="Processing threads"
            ):
                letter = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        f"Thread for letter '{letter}' encountered a fatal error."
                    )
    else:
        logger.info("Processing sequentially...")
        for letter in tqdm(letters, desc="Processing sequentially"):
            aggregate_letter(schema_path, hdf5_root, avro_dir, letter)

    logger.info("Conversion completed.")


if __name__ == "__main__":
    main()
