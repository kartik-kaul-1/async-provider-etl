import asyncio
from collections.abc import Coroutine, Mapping
from aiohttp.web import HTTPError
from datetime import datetime
from multiprocessing import Pool, cpu_count
from aiohttp import ClientSession
from aiosqlite import Cursor
from pathlib import Path

# custom python package imports
from .file_processing import process_and_write_file

__all__ = ["assemble_concurrent_awaitables", "process_files_in_parallel"]


async def assemble_concurrent_awaitables(
    cursor: Cursor,
    session: ClientSession,
    datasets: list[Mapping],
    raw_directory: Path,
    *,
    verbose: bool = True,
) -> list[Coroutine]:
    """asynchronous function that generates a list of coroutines to be executed concurrently in an asyncio event loop

    Args:
        cursor (Cursor): Cursor instance used to interact with SQLite
        session (ClientSession): ClientSession instance for making asynchronous HTTP requests via aiohttp
        datasets (list[Mapping]): JSON records that match "hospital" theme
        raw_directory (Path): represents the directory where the downloaded files will be saved
        verbose (bool): determines whether to print information to the console. Defaults to `True`

    Returns:
        list[Coroutine]: list to store download coroutines
    """
    download_tasks = []
    # iterate over all Hospital theme records
    for dataset in datasets:
        # get the title of the file
        file_name = dataset["title"]
        # get the last modified date of the CSV and parse it as a `datetime` object
        csv_last_modified = datetime.strptime(dataset["modified"], "%Y-%m-%d")
        # get the mapping from which we can extract the URL
        for distribution in dataset["distribution"]:
            # we'll use the URL to submit concurrent GET request to download
            file_url = distribution["downloadURL"]
            # Check if the file's last_modified time stamp record exists in the SQLite metadata table
            # if it doesn't we would have to process the file
            await cursor.execute(
                "SELECT last_modified FROM metadata WHERE file_name = ?",
                (file_name,),  # SQLite preferred method to avoid injections
            )
            # get the date the file was last modified; since cell so use index 0 eventually to capture value
            result = await cursor.fetchone()
            # if the record exists
            if result:
                # convert the string value into a datetime object
                last_modified = datetime.strptime(result[0], "%Y-%m-%d")
                # check whether it's been previously downloaded and if so, whether the modification is unaccounted for
                # (assuming daily batch jobs as specified in requirements; skip repeated run on same day)
                if csv_last_modified.date() <= last_modified.date():
                    if verbose:
                        print(
                            f"Skipping '{file_name}' as it was already processed and downloaded in the past."
                        )
                    # if so, skip; no coroutine to add to the event loop
                    continue
            # if we haven't seen the record before, or if it has been modified recently
            # then add it as a coroutine objects that has to be scheduled on event loop
            download_tasks.append(
                download_and_save_file(
                    session, file_url, file_name, raw_directory, verbose
                )
            )
    # the coroutine objects will be wrapped in a task and start running concurrently in `process_files_in_parallel`
    return download_tasks


async def download_and_save_file(
    session: ClientSession,
    file_url: str,
    file_name: str,
    raw_directory: Path,
    verbose: bool = True,
) -> Path:
    """Coroutine that uses the connection pool to submit non-blocking concurrent I/O requests to CMS server to
    download and write received data to local file system in a piece wise manner to prevent out of memory errors

    Args:
        session (ClientSession): ClientSession instance for making asynchronous HTTP requests via aiohttp
        file_url (str): URL of the CMS file to download from the web server
        file_name (str): name for the CSV file on the local file system
        raw_directory (Path): represents the directory where the downloaded files will be saved
        verbose(bool): determines whether to print information to the console. Defaults to `True`

    Raises: HTTPError: if the GET request wasn't able to be successfully completed; error handling has a lot more
    scope to be improved on

    Returns:
        Path: object representing a Path instance that includes the POSIX path of the CSV file
    """
    # the ClientSession is an asynchronous context manager; use its get method to make the non-blocking network request
    async with session.get(file_url) as response:
        # if the request was a success
        if response.status == 200:
            # define where the downloaded file will be saved on the local file system
            file_path = raw_directory / (file_name + ".csv")
            # enter into a context manager that'll be used to write the contents of the CSV file in binary
            with file_path.open("wb") as file:
                # Save the file by reading 1KB chunks then writing each chunk to disc before next request
                # prevents a single massive file overloading system memory
                async for chunk in response.content.iter_chunked(n=1024):
                    file.write(chunk)

            if verbose:
                print(f"Downloaded: {file_name}.")
            # return a Path object containing the POSIX path of the downloaded file
            return file_path
        else:
            # in case of error raise an exception; this needs to be made much more robust
            # asyncio has many error handling that can improve on this.
            raise HTTPError(
                text=f"Failed to download {file_name} @ the following URL: {file_url}"
            )


async def process_files_in_parallel(
    download_tasks: list[Coroutine],
    transformed_dir: Path,
    *,
    num_processes: int = cpu_count(),
    verbose: bool = True,
) -> list[Path]:
    """Utilizes the multiprocessing module to spin a pool of workers to process the downloaded files in parallel

    Args:
        download_tasks (list[Coroutine]): list of coroutine objects that will be processed in a non-blocking fashion
        transformed_dir (Path): represents the directory where the transformed files will be persisted
        num_processes (int): number of worker processes in the process pool. Defaults to `cpu_count()`
        verbose(bool): determines whether to print information to the console. Defaults to `True`
    Returns:
        list[Path]: list of Path objects that were processed, whose entries in the metadata database has to be updated
    """
    # open a process pool context manager, by default as many worker processes as there are cores will be spun up
    processing_tasks = []
    with Pool(num_processes) as pool:
        if verbose:
            print(
                f"Total number of workers currently in the process pool are: {pool._processes}.\n"
            )
        # takes in list of awaitables and returns an iterator of futures which can be iterated over awaiting each one
        # 😵‍💫 essentially: process files as they get downloaded as opposed to waiting for all of them to be downloaded.
        for coroutine in asyncio.as_completed(download_tasks):
            file_path = await coroutine
            # record the file that was download and which is scheduled to be processed
            # metadata_update_files.append(file_path)
            # submit the processing task to the process pool to be processed in parallel
            processing_task = pool.apply_async(
                process_and_write_file,
                (file_path, transformed_dir, verbose),
            )
            # record the task submitted to the processor pool
            processing_tasks.append(processing_task)

        # creates a list of Path objects whose metadata has to be updated in the SQLite database
        metadata_update_files = [
            # return the result of calls to `process_and_write_file` function (i.e. `Path` objects of files processed)
            task.get()
            for task in processing_tasks
        ]

    return metadata_update_files
