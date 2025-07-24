import sys
import pandas as pd
import sqlite3
from typing import Dict, Any, List
from loguru import logger
from enum import Enum


class Color(Enum):
    WHITE = "WHITE"  # unfound
    RED = "RED"  # found but neighbours not yet processed
    BLACK = "BLACK"  # found and neighbours all processed


def create_graph_from_db(
    starting_artist_id: str, db_path: str, output_path: str
) -> None:
    logger.info(f"Retriving data from {db_path}...")
    logger.info(f"Creating graph for artist {starting_artist_id}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        query = """
        SELECT a.artist_id, GROUP_CONCAT(s.similar)
        FROM artists a
        LEFT JOIN similarity s ON a.artist_id = s.target
        GROUP BY a.artist_id
        """
        cursor.execute(query)
        results = cursor.fetchall()
        if not results:
            logger.warning("No data found in the database.")
            conn.close()
            sys.exit(1)
        conn.close()
        graph_data_list: List[Any] = []
        for artist_id, similar_artists in results:
            connections = similar_artists if similar_artists else ""
            color = Color.WHITE.value
            dist = 999999

            if artist_id == starting_artist_id:
                color = Color.RED.value
                dist = 0

            graph_data_list.append(
                {
                    "artist_id": artist_id,
                    "connections": connections,
                    "color": color,
                    "dist": dist,
                }
            )
        df = pd.DataFrame(graph_data_list)
        df = df.astype(
            {
                "artist_id": "string",
                "connections": "string",
                "dist": "int32",
                "color": "category",
            }
        )
        df.to_parquet(output_path, index=False, engine="pyarrow")
        logger.info(f"Graph data saved to {output_path}")
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        sys.exit(1)
    except FileNotFoundError:
        logger.error(f"Database file not found: {db_path}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    return
