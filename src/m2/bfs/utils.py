import sys
import sqlite3
import os
from typing import Dict, Any, List


def get_artist_neighbor(artist_id: str, db_path: str) -> List[str]:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    query = f"""
        SELECT similar FROM similarity
        WHERE target = \"{artist_id}\"
    """
    results = cursor.execute(query).fetchall()
    return [row[0] for row in results]
