import asyncio
from datetime import datetime
from pathlib import Path
from aiosqlite import Cursor, Connection

__all__ = ["update_metadata_for_files", "update_sqlite_metadata"]


async def update_metadata_for_files(
    metadata_update_files: list[Path], conn: Connection, verbose: bool = True
) -> None:
    async with conn.cursor() as cursor:
        """Asynchronously updates the SQLite metadata database records for the processed files

    Args:
        metadata_update_files (list[Path]): list of file paths for which metadata needs to be updated
        conn (Connection): Connection proxy instance used to interact with SQLite
        verbose (bool): determines whether to print information to the console. Defaults to `True`
    """
        # create a list of tasks that will be scheduled for execution on the event loop
        tasks = [
            # for each file processed, create tasks out of the coroutine objects returned from the update function
            asyncio.create_task(update_sqlite_metadata(file.stem, cursor, verbose))
            for file in metadata_update_files
        ]
        # wait for all the update tasks to complete
        await asyncio.gather(*tasks)


async def update_sqlite_metadata(
    file_name: str, cursor: Cursor, verbose: bool = True
) -> None:
    """updates the last modified data for the given file that's being processed to record pipeline activity metadata

    Args:
        file_name (str): name of the file which we'll use to identify the `file_name` record
        cursor (Cursor): Cursor instance used to interact with SQLite asynchronously
        verbose: determines whether to print information to the console. Defaults to `True`
    """
    if verbose:
        print(
            f"Submitting update request to the SQLite metadata database for the record corresponding to {file_name}."
        )
    await cursor.execute(
        """
            INSERT OR REPLACE INTO metadata (file_name, last_modified)
            VALUES (?, ?)
        """,
        (file_name, datetime.now().strftime("%Y-%m-%d")),
    )
    if verbose:
        print(
            f"Updated the entry corresponding to {file_name} in the SQLite metadata database."
        )
