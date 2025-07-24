import argparse
import sys
from loguru import logger
from typing import List, Tuple

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


def parser() -> Tuple[str, str, str]:
    parsers = argparse.ArgumentParser(
        description="""
            Running BFS algorithms for artists graph
            to find similar songs based on similar artists.
        """
    )
    parsers.add_argument(
        "-m",
        "--mode",
        type=str,
        required=True,
        choices=["spark", "mapreduce"],
        dest="mode",
        default="mapreduce",
        help="Mode of execution: spark or mapreduce",
    )
    parsers.add_argument(
        "-d",
        "--db_path",
        type=str,
        required=True,
        dest="db_path",
        default="./data/artist_similarity.db",
        help="Path to the SQLite database containing artist similarity data",
    )
    parsers.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        choices=["local", "cluster"],
        dest="config",
        default="local",
        help="Running mode: local or cluster",
    )
    args = parsers.parse_args()
    logger.info(
        "Mode: {} \nDatabase Path: {} \nConfiguration: {}".format(
            args.mode, args.db_path, args.config
        )
    )
    if not args.schema or not args.hdf5 or not args.avro:
        logger.error("Schema, input, and output paths are required.")
        sys.exit(1)
    logger.info("Arguments parsed successfully.")
    return args.mode, args.db_path, args.config


if __name__ == "__main__":
    mode, db_path, running_mode = parser()
