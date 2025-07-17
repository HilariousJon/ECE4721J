#!/usr/bin/env python3
import fastavro
import os
import numpy as np
import sys
import argparse
from typing import List, Dict, Any, Tuple
from tqdm import tqdm
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema as avroschema
from avro.schema import UnionSchema, PrimitiveSchema
from loguru import logger
import hdf5_getters
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    parser = argparse.ArgumentParser(description="Convert MSD HDF5 to Avro")
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


def get_field_type(field: Any) -> str | None:
    base_type = None
    if isinstance(field.type, list):
        for t in field.type:
            if str(t).lower() != "null":
                base_type = t
                break
    elif isinstance(field.type, dict):
        # handle specifically the case of segments_timbre
        base_type = field.type.get("type")
    elif isinstance(field.type, UnionSchema):
        for option in field.type.schemas:
            if isinstance(option, PrimitiveSchema):
                base_type = option.type
                break
    elif isinstance(field.type, PrimitiveSchema):
        base_type = field.type.type
    else:
        base_type = str(field.type)
    t = str(base_type).lower()
    if t == "string":
        return "str"
    if t in ("int", "long"):
        return "int"
    if t in ("float", "double"):
        return "float"
    if t == "array":
        return "array"
    return None


def extract_hdf5_data(h5_path: str, schema: Any) -> Dict[str, Any]:
    h5 = hdf5_getters.open_h5_file_read(h5_path)
    record: Dict[str, Any] = {}
    try:
        for field in schema.fields:
            getter_name = "get_" + field.name
            if not hasattr(hdf5_getters, getter_name):
                continue
            try:
                raw_value = getattr(hdf5_getters, getter_name)(h5)
                ftype = get_field_type(field)
                if ftype == "str":
                    record[field.name] = str(raw_value)
                elif ftype == "int":
                    record[field.name] = int(raw_value)
                elif ftype == "float":
                    record[field.name] = float(raw_value)
                elif ftype == "array":
                    record[field.name] = raw_value
                else:
                    record[field.name] = raw_value
            except Exception as e:
                logger.error(f"Error reading {field.name}: {e}")
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


def aggregate_letter(
    schema_path: str, hdf5_root: str, avro_dir: str, letter: str
) -> None:
    schema = avroschema.parse(open(schema_path, "rb").read())
    subfolder = os.path.join(hdf5_root, letter)
    output_file = os.path.join(avro_dir, f"{letter}.avro")
    h5_files = find_all_h5_files(subfolder)
    if not h5_files:
        return
    with DataFileWriter(open(output_file, "wb"), DatumWriter(), schema) as writer:
        for h5 in h5_files:
            rec = extract_hdf5_data(h5, schema)
            if rec:
                writer.append(rec)
    logger.info(f"[{letter}] written to {output_file}")


def main():
    schema_path, hdf5_root, avro_dir, num_threads = parse_args()
    logger.info("Starting conversion")
    letters = [
        d for d in os.listdir(hdf5_root) if os.path.isdir(os.path.join(hdf5_root, d))
    ]
    if num_threads and num_threads > 0:
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
                    logger.error(f"[{letter}] failed: {e}")
    else:
        for letter in tqdm(letters, desc="Processing sequentially"):
            aggregate_letter(schema_path, hdf5_root, avro_dir, letter)
    logger.info("Conversion completed")


if __name__ == "__main__":
    main()
