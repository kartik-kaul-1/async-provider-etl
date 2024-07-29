import polars as pl
from pathlib import Path
from string import ascii_letters, digits
from itertools import chain

__all__ = ["process_and_write_file"]


def process_and_write_file(
    file_path: Path, transformed_directory: Path, verbose: bool = True
) -> Path:
    """processed the raw CSV file that was downloaded; renamed the header row of the file and then persists it,
    to local file system

    Args:
        file_path (Path): file path object for the raw file to process
        transformed_directory (Path): represents the directory where the processed files will be persisted
        verbose (bool): determines whether to print information to the console. Defaults to `True`
    """
    if verbose:
        print(f"Started processing {file_path.stem} file.")
    # Valid character set for column names: includes letters, digits and the underscore character
    valid_char_set = set(chain(ascii_letters, digits, ["_"]))
    # Process the downloaded file using Polars; read field entries as strings by setting `infer_schema_length`
    df = pl.read_csv(file_path, infer_schema_length=0)
    # Convert column names to snake_case; hence assumes special characters not allowed in column names
    # polars needs a mapping from old column name to new one, as input for the `rename` method
    column_mapping = {
        col: "".join(
            filter(lambda char: char in valid_char_set, col.lower().replace(" ", "_"))
        )
        for col in df.columns
    }
    df = df.rename(column_mapping)
    # provide the file system path where the transformed file will be placed
    transformed_path = transformed_directory / file_path.name
    # Save the processed CSV file on to the file system
    df.write_csv(transformed_path)

    if verbose:
        print(f"Transformed and persisted {transformed_path.name} file.")

    return file_path
