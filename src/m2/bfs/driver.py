import argparse
import sys
import os
from loguru import logger
from typing import Tuple
try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
except NameError:
    sys.path.insert(0, os.path.abspath('.'))
from src.m2.bfs.spark_bfs import run_bfs_spark

logger.remove()
logger.add(
    sys.stderr,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>",
)


def setup_parsers() -> Tuple[str, str, str, str, str, str, int]:
    parser = argparse.ArgumentParser(
        description="Running BFS algorithms for artists graph to find similar songs."
    )
    parser.add_argument(
        "-m",
        "--mode",
        type=str,
        required=True,
        choices=["spark", "mapreduce"],
        dest="mode",
        default="spark",
        help="Mode of execution",
    )
    parser.add_argument(
        "-a",
        "--artist_db_path",
        type=str,
        required=True,
        dest="artist_db_path",
        help="Path to the SQLite DB for artist similarity",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        choices=["local", "cluster"],
        dest="config",
        default="local",
        help="Running mode: local or cluster",
    )
    parser.add_argument(
        "-i",
        "--avro",
        type=str,
        required=True,
        dest="avro",
        help="Path to the avro file that stores the song feature data",
    )
    parser.add_argument(
        "-s",
        "--song_id",
        type=str,
        required=True,
        dest="song_id",
        help="Track ID to start the BFS traversal from",
    )
    parser.add_argument(
        "-M",
        "--meta_db_path",
        type=str,
        required=True,
        dest="meta_db_path",
        help="Path to the SQLite DB for track metadata",
    )
    parser.add_argument(
        "-D",
        "--bfs_depth",
        type=int,
        required=False,
        dest="bfs_depth",
        default=2,
        help="Depth of BFS to traverse the artist graph",
    )

    args = parser.parse_args()

    if args.bfs_depth < 1:
        logger.error("BFS depth must be at least 1.")
        sys.exit(1)

    logger.info("Arguments parsed successfully.")
    return (
        args.mode,
        args.artist_db_path,
        args.config,
        args.avro,
        args.song_id,
        args.meta_db_path,
        args.bfs_depth,
    )


if __name__ == "__main__":
    run_bfs_spark(setup_parsers())
