import sqlite3
import asyncio
import threading
from argparse import ArgumentParser
from multiprocessing import cpu_count
from timeit import timeit
from typing import Final, Annotated
from aiohttp import ClientSession
from aiosqlite import connect, Cursor
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

# custom python module imports
from .util import (
    assemble_concurrent_awaitables,
    process_files_in_parallel,
    read_csv,
    write_to_sqlite,
    update_metadata_for_files,
)

# Configuration settings; in the future USE `argparse` to configure this to be run from CLI
BASE_URL: Final[str] = (
    "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
)
OUTPUT_DIR: Final[Path] = Path.cwd() / "data/hospital_files"
# Create the output directory if it doesn't exist
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
# Create the RAW directory if it doesn't exist
RAW_DIR: Final[Path] = OUTPUT_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
# Create the TRANSFORMED directory if it doesn't exist
TRANSFORMED_DIR: Final[Path] = OUTPUT_DIR / "transformed"
TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)
# Create the SQLite directory if it doesn't exist
METADATA_DB_PATH: Final[Path] = Path.cwd() / "data/metadata_database"
METADATA_DB_PATH.mkdir(parents=True, exist_ok=True)
# name of the metadata database
METADATA_DB_NAME: Final[str] = "metadata.db"
# hospital_database is the name of the SQLite database that will store the transformed data
HOSPITAL_DB_PATH: Final[Path] = Path.cwd() / "data/hospital_database"
HOSPITAL_DB_PATH.mkdir(parents=True, exist_ok=True)
# name of the metadata database
HOSPITAL_DB_NAME: Final[str] = "hospital.db"

# Parse CLI arguments
parser = ArgumentParser(description="ETL job configuration")
parser.add_argument(
    "--print-updates-to-console",
    action="store_true",
    help="Determines whether to print the updates of the ETL job to the console",
)
args = parser.parse_args()
# verbose logging setting
print_updates_to_console: Annotated[
    bool, "Determines whether to print the updates of the ETL job to the console"
] = args.print_updates_to_console


async def main() -> None:
    # Create the metadata database if it doesn't exist
    metadata_db_file = METADATA_DB_PATH / METADATA_DB_NAME
    # Connect to the SQLite database asynchronously using aiosqlite
    async with connect(metadata_db_file) as conn:
        # Create the metadata table if it doesn't exist
        await conn.execute(
            """CREATE TABLE IF NOT EXISTS metadata (
                file_name TEXT PRIMARY KEY,
                last_modified TIMESTAMP
            )"""
        )
        # Commit the changes
        await conn.commit()

    # create client session using a reusable pool of collections via aiohttp; use session to submit concurrent requests
    async with ClientSession() as session:
        # params={"theme": "Hospitals"} search query doesn't seem to work,
        async with session.get(BASE_URL) as response:
            # so entire JSON file has to be downloaded; wait till the file has been downloaded
            all_datasets = await response.json()
        # Fetch/filter the list of datasets related to the "Hospitals" theme
        datasets = [
            dataset for dataset in all_datasets if "Hospitals" in dataset.get("theme")
        ]
        # re-establish the asynchronous connection with the metadata database
        async with connect(metadata_db_file) as conn:
            # create a cursor instance that we'll use throughout our asyncio application
            cursor: Cursor = await conn.cursor()
            # generate a list of coroutine objects to be executed asynchronously
            download_tasks = await assemble_concurrent_awaitables(
                cursor, session, datasets, RAW_DIR, verbose=print_updates_to_console
            )
        # check if there are even files to download
        if download_tasks:
            print(
                f"\nBeginning EXTRACT and TRANSFORM operations from `{BASE_URL}` using the SQLite metadata database.\n"
            )

            # Process the downloaded files using a process pool to enable multiprocessing
            # files are stored after processing into `TRANSFORMED` directory
            updated_files = await process_files_in_parallel(
                download_tasks, TRANSFORMED_DIR, verbose=print_updates_to_console
            )
            # re-establish the asynchronous connection with the metadata database
            async with connect(metadata_db_file) as conn:
                # Enable Write-Ahead Logging (WAL) mode for SQLite to optimize concurrent writes
                await conn.execute("PRAGMA journal_mode=WAL;")
                # log the run in the metadata repository to avoid duplicate runs in the future
                await update_metadata_for_files(
                    updated_files, conn, verbose=print_updates_to_console
                )
                # Commit the changes applied
                await conn.commit()
        else:
            print("No new files to process.")
            # exit out of the coroutine if there are no files to process
            return

    print(
        f"Completed EXTRACT and TRANSFORM operations; CSV files stored in `{TRANSFORMED_DIR}`.",
        "Now beginning LOAD operation to hospital SQLite database.\n",
        sep="\n",
    )

    hospital_db_file = HOSPITAL_DB_PATH / HOSPITAL_DB_NAME
    loop = asyncio.get_running_loop()
    csv_file_paths = [file for file in updated_files if file.suffix == ".csv"]
    # Utilize multiprocessing to read CSV files in parallel
    with ProcessPoolExecutor(max_workers=cpu_count()) as process_executor:
        if print_updates_to_console:
            print(
                f"\nTotal number of workers currently in the process pool are: {process_executor._processes}.\n"
            )
        read_tasks = [
            loop.run_in_executor(
                process_executor, read_csv, file_path, print_updates_to_console
            )
            for file_path in csv_file_paths
        ]
        read_results = await asyncio.gather(*read_tasks)

    write_tasks = []
    # Create a Lock to ensure that only one thread writes to the SQLite database at a time
    db_lock = threading.Lock()
    # check_same_thread=False is required for concurrent access to the SQLite database
    conn = sqlite3.connect(hospital_db_file, check_same_thread=False)
    # Enable Write-Ahead Logging (WAL) mode for SQLite to optimize concurrent writes
    conn.execute("PRAGMA journal_mode=WAL;")

    with ThreadPoolExecutor(max_workers=cpu_count()) as thread_executor:
        if print_updates_to_console:
            print(
                f"\nTotal number of workers currently in the thread pool are: {thread_executor._max_workers}.\n"
            )
        for result in read_results:
            if result is not None:
                table_name, df = result
                write_tasks.append(
                    loop.run_in_executor(
                        thread_executor,
                        write_to_sqlite,
                        table_name,
                        df,
                        db_lock,
                        conn,
                        print_updates_to_console,
                    )
                )
            else:
                print("Failed to read a file.")
    # Wait for all writes to complete
    await asyncio.gather(*write_tasks)

    print(
        f"Completed LOAD operation to hospital SQLite database located at `{hospital_db_file}`."
    )

    conn.close()

    print(
        "\nALL previously unobserved or recently modified files have been downloaded and processed.",
        f"The SQLite metadata database located at `{metadata_db_file}` has also been updated with today's load.",
        sep="\n",
    )
