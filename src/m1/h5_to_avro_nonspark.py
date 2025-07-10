import fastavro
import os
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

logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>",
)


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
        help="""Output folder path for the aggregate avro files.
        Final result will be in /path/to/output/aggregate.avro""",
    )
    parsers.add_argument(
        "-s",
        "--schema",
        type=str,
        required=True,
        dest="schema",
        help="Path to the Avro schema file (.json or .avsc)",
    )
    args = parsers.parse_args()

    if args.schema and args.hdf5 and args.avro:
        logger.info(f"Schema: {args.schema}\nInput: {args.hdf5}\nOutput: {args.avro}\n")
    else:
        logger.error("Missing required arguments.")
        parsers.print_help()
        exit(1)

    if not os.path.exists(args.hdf5):
        logger.error(f"Input directory {args.hdf5} does not exist.")
        exit(1)
    if not os.path.exists(args.schema):
        logger.error(f"Schema file {args.schema} does not exist.")
        exit(1)
    if os.path.exists(args.avro):
        logger.warning(f"Output directory {args.avro} already exists. It will be used.")
    else:
        os.makedirs(args.avro)

    return [args.schema, args.hdf5, args.avro]


def merge_avro_files(hdf5_path: str, avro_folder: str) -> Tuple[Any, List[Any]]:
    results: List[Any] = []
    schema: Any = None
    for letter in tqdm(
        os.listdir(hdf5_path), desc="Reading Avro files", unit="file", leave=False
    ):
        avro_file_path = os.path.join(avro_folder, f"{letter}.avro")
        if not os.path.exists(avro_file_path):
            continue
        with open(avro_file_path, "rb") as avro_f:
            reader = fastavro.reader(avro_f)
            records = list(reader)
            if results:
                results.extend(records)
            else:
                results = records
                schema = reader.schema
    return schema, results


def get_field_type(field: Any) -> str | None:
    base_type = None

    if isinstance(field.type, list):
        for t in field.type:
            if str(t).lower() != "null":
                base_type = t
                break
    elif isinstance(field.type, UnionSchema):
        for option in field.type.schemas:
            if isinstance(option, PrimitiveSchema):
                base_type = option.type
                break
    elif isinstance(field.type, PrimitiveSchema):
        base_type = field.type.type
    else:
        base_type = str(field.type)

    base_type = str(base_type).lower()
    if base_type == "string":
        return "str"
    elif base_type in ("int", "long"):
        return "int"
    elif base_type in ("float", "double"):
        return "float"
    else:
        return None


def extract_hdf5_data(h5file: str, schema: Any) -> Dict[str, Any]:
    hdf5 = hdf5_getters.open_h5_file_read(h5file)
    fields = schema.fields
    song_record: Dict[str, Any] = {}
    for field in fields:
        current_getter = "get_" + field.name
        if not hasattr(hdf5_getters, current_getter):
            logger.warning(f"Field {field.name} has no corresponding getter.")
            continue
        try:
            value = getattr(hdf5_getters, current_getter)(hdf5)
            type_ = get_field_type(field)
            if type_ == "str":
                song_record[field.name] = str(value)
            elif type_ == "int":
                song_record[field.name] = int(value)
            elif type_ == "float":
                song_record[field.name] = float(value)
            else:
                song_record[field.name] = value
                logger.debug(f"Extracted {field.name}: {value}")
        except Exception as e:
            logger.error(f"Error processing field {field.name}: {e}")
            continue
    return song_record


def find_all_h5_files(folder_path: str) -> List[str]:
    return [
        os.path.join(root, h5file)
        for root, _, files in os.walk(folder_path)
        for h5file in files
        if h5file.endswith(".h5") or h5file.endswith(".hdf5")
    ]


def aggregate_hdf5_to_avro(
    schema_path: str, hdf5_path: str, avro_path: str, letter: str
) -> None:
    schema = avroschema.parse(open(schema_path, "rb").read())
    folder_path = os.path.join(hdf5_path, letter)
    avro_file_path = os.path.join(avro_path, f"{letter}.avro")
    h5files: List[str] = find_all_h5_files(folder_path)
    if not h5files:
        logger.warning(f"No .h5 files found in {folder_path}")
        return
    with DataFileWriter(
        open(avro_file_path, "wb"),
        DatumWriter(),
        schema,
    ) as writer:
        for h5file in h5files:
            record: Dict[str, Any] = extract_hdf5_data(h5file, schema)
            if record:
                writer.append(record)
    logger.info(f"Finished processing {letter}. Output: {avro_file_path}")


if __name__ == "__main__":
    schema_path, hdf5_root, avro_output = parser()
    logger.info("Starting conversion from HDF5 to Avro")

    try:
        for fname in tqdm(os.listdir(hdf5_root), desc="Processing subfolders"):
            aggregate_hdf5_to_avro(schema_path, hdf5_root, avro_output, fname)

        merged_schema, merged_results = merge_avro_files(hdf5_root, avro_output)
        output_file = os.path.join(avro_output, "aggregate.avro")
        with open(output_file, "wb") as f:
            fastavro.write(f, merged_schema, merged_results)
        logger.success(f"Aggregate Avro file written to: {output_file}")

    except Exception as e:
        logger.exception(f"Fatal error during conversion: {e}")
        sys.exit(1)

    logger.info("Conversion completed successfully.")
