from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def normalized_string(column_name: str):
    """
    Convert a DataFrame column into a normalized string representation.
    """
    return f.coalesce(
        f.trim(f.col(column_name).cast("string")),
        f.lit("<null>")
    )


def add_record_hash(df: DataFrame, tracked_columns: Sequence[str], output_column: str = "record_hash"):
    """
    Add a deterministic SHA-256 hash column to a DataFrame.
    The hash is calculated from the columns listed in tracked_columns.

    Parameters:
    df: DataFrame to which the hash column will be added.
    tracked_columns: Ordered collection of column names used to calculate the hash.
    output_column: Name of the generated hash column. The default is "record_hash".

    Returns: 
    DataFrame : Original DataFrame with the generated hash column appended.
    """

    # Fail early to avoid creating the same constant hash for every row if no columns are given 
    if not tracked_columns:
        raise ValueError("tracked_columns cannot be empty")

    # Validate that every requested tracked column exists in the DataFrame.
    # The set difference returns requested columns that are missing.
    missing_columns = set(tracked_columns) - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"Columns missing from DataFrame: {sorted(missing_columns)}"
        )

    # Convert every tracked column into a consistently normalized string.
    values = [
        normalized_string(column_name)
        for column_name in tracked_columns
    ]

    # concat_ws() combines all normalized values into one string using "||" as a separator.
    return df.withColumn(
        output_column,
        f.sha2(f.concat_ws("||", *values), 256)
    )