from pyspark.sql import DataFrame
from pyspark.sql import functions as f
import olympic_pipeline.basic as u


def check_count_rows(df:DataFrame, name:str):
    """
    Count and log the number of rows processed.

    Returns:
        The number of rows in the DataFrame.
    """    
    count = df.count()
    return u.log(f"Total number of rows processed from {name}: {count}")


def check_not_empty(df: DataFrame, table_name: str):
    """
    Raise an error when the DataFrame contains no rows.
    """
    if df.count() == 0:
        raise ValueError(f"{table_name} is empty")
    else:
        u.log(f"Table {table_name} contains {df.count()} rows")


def check_no_null_keys(df: DataFrame, key_columns: list[str], table_name: str):
    """
    Check that key columns do not contain null or blank values.
    """
    null_condition = f.lit(False)

    for column_name in key_columns:
        null_condition = null_condition | f.col(column_name).isNull()

    null_count = df.filter(null_condition).count()

    if null_count > 0:
        raise ValueError(
            f"{table_name} contains {null_count} rows with null keys!"
        )
    else:
        u.log(f"There are NO null keys in {table_name}")


def check_one_current_noc_version(dim_noc: DataFrame):
    """
    Ensure each NOC code has at most one current dimension version.
    """
    invalid = (
        dim_noc.filter(f.col("is_current"))
        .groupBy("noc_code")
        .count()
        .filter(f.col("count") > 1)
        .count()
    )

    if invalid > 0:
        raise ValueError(
            "dim_noc contains multiple current versions for the same NOC !"
        )
    else:
        u.log("dim_noc contains only one current version for every NOC code")

    
def check_noc_reference_coverage(silver_athletes: DataFrame, silver_noc: DataFrame):
    """
    Ensure every athlete NOC code exists in the canonical NOC reference.
    """
    unmatched_nocs = (
        silver_athletes
        .groupBy("noc_code")
        .count()
        .join(
            silver_noc
            .select("noc_code")
            .distinct(),
            on="noc_code",
            how="left_anti",
        )
        .orderBy(f.desc("count"))
    )

    if unmatched_nocs.count() > 0:
        unmatched_nocs.show(20,truncate=False)

        raise ValueError( "Athlete records contain NOC codes that are missing from the canonical NOC reference.")
    else:
        u.log("Every athlete NOC code have a match in the noc_regions reference table.")
    
def check_foreign_key_integrity( fact_df: DataFrame, dimension_df: DataFrame, key_column: str, dimension_name: str, fact_name : str):
    missing_keys = (
        fact_df
        .select(key_column)
        .filter(f.col(key_column).isNotNull())
        .distinct()
        .join(
            dimension_df
            .select(key_column)
            .distinct(),
            on=key_column,
            how="left_anti"
        )
    )

    missing_count = missing_keys.count()

    if missing_count > 0:

        missing_keys.show(20, truncate=False)

        raise ValueError( f"{missing_count} {key_column} values from the fact table are missing from {dimension_name}")
    else:
        u.log(f"Foreign key integrity check passed: every {key_column} value present in {fact_name} matches an existing primary key in {dimension_name}. ")