import fastavro
import h5py
import os
import argparse
from typing import List
from tqdm import tqdm
from pyspark import SparkContext
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from loguru import logger as logger
import hdf5_getters


def parser() -> List[str]:
    parsers = argparse.ArgumentParser(
        description="Convert MSD HDF5 files to more concise avro file"
    )
    parsers.add_argument(
        "-i",
        "--input",
        type=str,
        required=True,
        dest="hdf5",
        help="Directory containing the source data of MSongDataset",
    )
    parsers.add_argument(
        "-o",
        "--output",
        type=str,
        required=True,
        dest="avro",
        help="output path for the aggregate avro files",
    )
    parsers.add_argument(
        "-s",
        "--schema",
        type=str,
        required=True,
        dest="schema",
        help="Path to the Avro schema file, typically a `json` or `avsc` file",
    )
    args = parsers.parse_args()

    if args.schema and args.hdf5 and args.avro:
        logger.info(
            "Schema: {} \nInput: {} \nOutput: {} \n".format(
                args.schema, args.hdf5, args.avro
            )
        )
    else:
        logger.error(
            "Missing required arguments. Please provide schema, input, and output paths."
        )
        parsers.print_help()
        exit(1)

    if not os.path.exists(args.hdf5):
        logger.error(f"Input directory {args.hdf5} does not exist.")
        exit(1)
    if not os.path.exists(args.schema):
        logger.error(f"Schema file {args.schema} does not exist.")
        exit(1)
    if os.path.exists(args.avro):
        logger.warning(
            f"Output directory {args.avro} already exists. It will be overwritten."
        )
    return [args.schema, args.hdf5, args.avro]


def merge_avro_files() -> None:
    return


def extract_hdf5_data() -> None:
    return


def aggregate_hdf5_to_avro() -> None:
    return


if __name__ == "__main__":
    parse_results: List[str] = parser()
    schema_path:str = parse_results[0]
    hdf5_path:str = parse_results[1]
    avro_path:str = parse_results[2]
    sc = SparkContext()
    logger.info("Starting conversion from HDF5 to Avro")
    
