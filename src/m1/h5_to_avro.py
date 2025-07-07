import fastavro
import os
import argparse
from typing import List, Dict, Any, Tuple
from tqdm import tqdm
from pyspark import SparkContext
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import avro.schema as avroschema
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
        help="""output folder path for the aggregate avro files.
        Final results will be in /path/to/your/output/aggregate.avro""",
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


def merge_avro_files(hdf5_path: str, avro_folder: str) -> Tuple[Any, List[Any]]:
    results: List[Any] = []
    schema: Any = None
    for letter in tqdm(
        os.listdir(hdf5_path), desc="Reading Avro files", unit="file", leave=False
    ):
        avro_file_path = avro_folder + f"/{letter}.avro"
        with open(avro_file_path, "rb") as avro_f:
            reader = fastavro.reader(avro_f)
            records = list(reader)
            if results:
                results.extend(records)
            else:
                results = records
                schema = reader.schema
    return schema, results


def get_field_type(field: Any) -> str:
    base_type = None

    if isinstance(field.type, list):
        for t in field.type:
            if t != "null":
                base_type = t
                break
    elif hasattr(field.type, "type"):
        base_type = field.type.type
    else:
        base_type = field.type

    base_type = str(base_type).lower()

    if base_type == "string":
        return "str"
    elif base_type == "int":
        return "int"
    elif base_type == "double":
        return "float"
    else:
        logger.warning(
            f"Unknown or unsupported field type: {base_type} for field {field.name}"
        )
        raise Exception(
            f"Unknown or unsupported field type: {base_type} for field {field.name}"
        )


def extract_hdf5_data(h5file: str, schema: Any) -> Dict[str, Any]:
    hdf5 = hdf5_getters.open_h5_file_read(h5file)
    fields = schema.fields
    song_record: Dict[str, Any] = {}
    # commented out the following loop when can runnig smoothly for the first time
    for field in fields:
        current_getter = "get_" + field.name
        if not hasattr(hdf5_getters, current_getter):
            logger.warning(
                f"Field {field.name} in avro files does not have a corresponding getter in hdf5_getters."
            )
            continue
        else:
            try:
                value = getattr(hdf5_getters, current_getter)(hdf5)
                type_ = get_field_type(field)
                if type_ == "str":
                    song_record[field.name] = str(value)
                elif type_ == "int":
                    song_record[field.name] = int(value)
                elif type_ == "double":
                    song_record[field.name] = float(value)
                else:
                    song_record[field.name] = value
                    logger.debug(f"Extracted {field.name}: {song_record[field.name]}")
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
    with DataFileWriter(
        open(avro_file_path, "wb"),
        DatumWriter(),
        schema,
    ) as writer:
        for h5file in h5files:
            record: Dict[str, Any] = extract_hdf5_data(h5file, schema)
            if record:
                writer.append(record)
    logger.info(
        f"Finished processing {letter} files. Output written to {avro_file_path}"
    )
    writer.close()
    return


if __name__ == "__main__":
    parse_results: List[str] = parser()
    sc = SparkContext()
    logger.info("Starting conversion from HDF5 to Avro")
    try:
        result_rdd = (
            sc.parallelize(os.listdir(parse_results[1])).map(
                lambda fname: aggregate_hdf5_to_avro(
                    parse_results[0], parse_results[1], parse_results[2], fname
                )
            )
        ).collect()
        logger.info("All files processed successfully.")
        merged_schema, merged_results = merge_avro_files(
            parse_results[1], parse_results[2]
        )
        with open(os.path.join(parse_results[2], "aggregate.avro"), "wb") as f:
            fastavro.write(f, merged_schema, merged_results)
        logger.info(
            "Aggregate Avro file created successfully at {}".format(
                os.path.join(parse_results[2], "aggregate.avro")
            )
        )
    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        sc.stop()
        exit(1)
    logger.info("Conversion completed successfully.")
    sc.stop()
