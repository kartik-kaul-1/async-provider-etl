# `async-provider-etl`

## Overview
`async-provider-etl` is a Python package designed to perform ETL operations on hospital CMS data using asynchronous 
and parallel processing. The package downloads datasets, processes them concurrently, and stores the hospital data & 
associated metadata in
SQLite databases.

## Features
- **Asynchronous Data Extraction**: Utilizes `aiohttp` for efficient, non-blocking HTTP requests to download datasets.
- **Parallel Data Processing**: Leverages `asyncio`, `ProcessPoolExecutor` and `ThreadPoolExecutor` for 
  concurrent and parallel 
  processing.
- **SQLite Integration**: Stores metadata and processed data in SQLite databases using `aiosqlite`, ensuring efficient, non-blocking queries to the embedded database.
- **Command-Line Interface**: Configurable via CLI arguments for verbose logging.

## Installation

> ### Prerequisite

  >If [`pipx`](https://github.com/pypa/pipx) is not already installed, you can install it using [`pipx-in-pipx`](https://github.com/mattsb42-meta/pipx-in-pipx):

  ```bash
  $ pip install pipx-in-pipx
  ```

To install the `.whl` file using `pipx`, you can use the following command:

```bash
$ pipx install "dist/async_provider_etl-0.1.0-py3-none-any.whl"
```
Then to trigger the ETL job, simply call the installed package:

```bash
$ async-provider-etl
```