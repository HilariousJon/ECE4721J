import os
import sys
import fastavro
from tqdm import tqdm
from loguru import logger

logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{message}</cyan>",
)


def merge_avro_files(avro_dir: str, output_filename: str = "aggregate.avro"):
    merged_records = []
    merged_schema = None

    avro_files = sorted(
        [
            f
            for f in os.listdir(avro_dir)
            if f.endswith(".avro") and f != output_filename
        ]
    )

    if not avro_files:
        logger.error("No .avro files found in the specified directory.")
        return

    for file in tqdm(avro_files, desc="Merging Avro", unit="file"):
        file_path = os.path.join(avro_dir, file)
        try:
            with open(file_path, "rb") as f:
                reader = fastavro.reader(f)
                if merged_schema is None:
                    merged_schema = reader.writer_schema
                merged_records.extend(reader)
        except Exception as e:
            logger.error(f"Failed to read {file}: {e}")

    if not merged_records:
        logger.error("No records to merge. Exiting.")
        return

    output_path = os.path.join(avro_dir, output_filename)
    try:
        with open(output_path, "wb") as out_f:
            fastavro.writer(out_f, merged_schema, merged_records)
        logger.success(f"Merged {len(merged_records)} records into {output_path}")
    except Exception as e:
        logger.error(f"Failed to write merged file: {e}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python merge_avro.py <avro_dir> [output_filename]")
        sys.exit(1)

    avro_dir = sys.argv[1]
    output_name = sys.argv[2] if len(sys.argv) > 2 else "aggregate.avro"
    merge_avro_files(avro_dir, output_name)
