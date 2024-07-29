from pathlib import Path
from sqlite3 import Connection
from threading import Lock

import pandas as pd

__all__ = ["read_csv", "write_to_sqlite"]


def read_csv(file_path: Path, verbose: bool = True) -> tuple[str, pd.DataFrame] | None:
    if verbose:
        print(f"Starting to read downloaded CSV file: {file_path}")
    try:
        df = pd.read_csv(file_path, dtype="unicode")
        table_name = file_path.stem
        if verbose:
            print(f"Successfully read downloaded CSV file: {file_path}")
        return table_name, df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def write_to_sqlite(
    table_name: str,
    df: pd.DataFrame,
    db_lock: Lock,
    conn: Connection,
    verbose: bool = True,
) -> None:
    # replace whitespace with underscores and lowercase the table name
    table_name = (
        table_name.lower()
        .replace(":", "")
        .replace("-", "")
        .replace("(", "")
        .replace(")", "")
        .replace(" - ", " ")
        .replace(" ", "_")
        .replace("__", "_")
    )
    if verbose:
        print(
            f"Starting to write CSV data to SQLite hospital database table: {table_name}"
        )
    with conn:
        if verbose:
            print(
                f"Establishing connection with SQLite hospital database for writes to table: {table_name}"
            )
        with db_lock:
            if verbose:
                print(
                    f"Retrieved lock to write to SQLite hospital database table: {table_name}."
                )
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            if verbose:
                print(
                    f"Released lock to allow other threads to write to SQLite hospital database."
                )
    if verbose:
        print(
            f"Successfully written CSV data to SQLite hospital database table: {table_name}"
        )
