from pathlib import Path
from datetime import date
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from olympic_pipeline.hashing import add_record_hash
from olympic_pipeline.reference_data import (
    apply_noc_mapping,
    clean_noc_mapping
)

def null_if_missing(column_name: str):
    """
    Standardize the different representations of missing values.

    Parameters:
    column_name: Name of the DataFrame column to normalize

    Returns:
    Column :Spark column expression containing either the cleaned value or null.
    """
    
    value = f.trim(f.col(column_name))

    return (
        f.when(
            value.isin("", "NA", "N/A", "null", "NULL"),
            f.lit(None)
        )
        .otherwise(value)
    )

def transform_athlete_events(df: DataFrame, noc_mapping: DataFrame):
    """
    Clean and standardize the Bronze athlete-events dataset.

    Responsibilities of this transformation include:

    - converting source strings into appropriate data types;
    - standardizing text values;
    - preserving the original NOC code;
    - applying the canonical NOC-code mapping;
    - removing unusable records and exact duplicates;
    - generating a deterministic record hash.

    Parameters:
    df: Bronze athlete-events DataFrame.
    noc_mapping: Cleaned reference mapping used to translate source-specific NOC codes into the canonical codes used by the platform.

    Returns: DataFrame : Cleaned Silver athlete-events DataFrame.
    """
    cleaned = (
        df.select(
            f.col("ID").cast("long").alias("athlete_id"),
            null_if_missing("Name").alias("athlete_name"),
            f.upper(null_if_missing("Sex")).alias("sex"),
            null_if_missing("Age").cast("integer").alias("age"),
            null_if_missing("Height").cast("double").alias("height_cm"),
            null_if_missing("Weight").cast("double").alias("weight_kg"),
            null_if_missing("Team").alias("team"),
            f.upper(null_if_missing("NOC")).alias("source_noc_code"),
            null_if_missing("Games").alias("games_name"),
            null_if_missing("Year").cast("integer").alias("year"),
            f.initcap(null_if_missing("Season")).alias("season"),
            null_if_missing("City").alias("host_city"),
            null_if_missing("Sport").alias("sport"),
            null_if_missing("Event").alias("event_name"),
            f.initcap(null_if_missing("Medal")).alias("medal"),
            f.col("_ingested_at"),
            f.col("_source_filename")
        )
        .filter(f.col("athlete_id").isNotNull())
        .filter(f.col("games_name").isNotNull())
        .filter(f.col("event_name").isNotNull())
        .dropDuplicates()
    )

    # Apply the governed NOC mapping after basic source cleaning.
    #
    # The mapping creates the canonical noc_code column while preserving
    # source_noc_code.
    #
    # Example:
    #   source_noc_code = SGP
    #   noc_code        = SGP
    #
    # When no mapping exists for a source value, apply_noc_mapping() keeps
    # the original code as the canonical value.
    mapped = apply_noc_mapping(df=cleaned, mapping=noc_mapping, source_system="athlete_events")

    return add_record_hash(
        df=mapped,
        tracked_columns=[
            "athlete_id",
            "games_name",
            "event_name",
            "team",
            "noc_code",
        ],
        output_column="source_record_hash"
    )

def transform_noc_regions(df: DataFrame, noc_mapping: DataFrame):
    """
    Clean and standardize the Bronze NOC-regions reference dataset.

    This transformation:

    - preserves the original source NOC code;
    - cleans the region and notes fields;
    - converts source-specific codes into canonical NOC codes;
    - removes duplicate canonical NOC records;
    - generates a hash used for SCD Type 2 change detection.

    Parameters:
    df: Bronze NOC-regions DataFrame.
    noc_mapping: Cleaned NOC-code mapping reference DataFrame.

    Returns: DataFrame : Cleaned Silver NOC-regions DataFrame.
    """
    cleaned = (
        df.select(
            # Preserve the original code supplied by noc_regions.csv.
            f.upper(null_if_missing("NOC")).alias("source_noc_code"),
            null_if_missing("region").alias("region"),
            null_if_missing("notes").alias("notes"),
            f.col("_ingested_at"),
            f.col("_source_filename")
        )
        # A reference row without an NOC code cannot be used as a dimension business-key record.        
        .filter(f.col("source_noc_code").isNotNull())
    )

    # Apply the source-specific reference mapping.
    #
    # For example:
    #   source_system   = noc_regions
    #   source_noc_code = SIN
    #   noc_code        = SGP
    #
    # This ensures that athlete records containing SGP can join to the
    # Singapore reference record.
    mapped = apply_noc_mapping(df=cleaned, mapping=noc_mapping, source_system="noc_regions")

    mapped = mapped.dropDuplicates(["noc_code"])

    #Only a change to region produces a different record_hash.
    return add_record_hash(
        df=mapped,
        tracked_columns=["region"]
    )


def build_silver(spark: SparkSession, bronze_path: Path, silver_path: Path, batch_date = date):
    """
    Build and persist the complete Silver layer.

    The function reads the raw Bronze datasets, cleans the NOC mapping,
    transforms both business datasets, writes the Silver Parquet outputs,
    and returns the resulting DataFrames for Gold processing.

    Parameters: 
        spark : Active Spark session.
        bronze_path: Root directory containing the Bronze Parquet datasets.
        silver_path: Root directory where the Silver Parquet datasets will be written.

    Returns:
        tuple[DataFrame, DataFrame] : The Silver athlete-events and NOC-regions DataFrames.
    """
    
    # ------------------------------------------------------------------
    # 1. Read Bronze operational datasets
    # ------------------------------------------------------------------
    bronze_athletes = (
        spark.read
        .parquet(str(bronze_path / "athlete_events"))
        .filter(
            f.col("_batch_date") == f.lit(batch_date).cast("date")
        )
    )

    bronze_noc = (
        spark.read
        .parquet(str(bronze_path / "noc_regions"))
        .filter(
            f.col("_batch_date") == f.lit(batch_date).cast("date")
        )
    )  

    # ------------------------------------------------------------------
    # 2. Read the governed NOC mapping reference
    # ------------------------------------------------------------------
    # The mapping was ingested into Bronze so the pipeline preserves the
    # exact reference-data version used by the batch.
    bronze_noc_mapping = (
        spark.read
        .parquet( str(bronze_path / "reference" / "noc_code_mapping"))
        .filter(
            f.col("_batch_date") == f.lit(batch_date).cast("date")
        )
    )

    # Standardize mapping values before applying them.
    #
    # Typical cleaning includes trimming whitespace and standardizing source systems and NOC codes to a consistent case.   
    noc_mapping = clean_noc_mapping(bronze_noc_mapping)
    
    # ------------------------------------------------------------------
    # 3. Transform athlete-events data
    # ------------------------------------------------------------------
    silver_athletes = transform_athlete_events(bronze_athletes, noc_mapping=noc_mapping)
    
    # ------------------------------------------------------------------
    # 4. Transform NOC reference data
    # ------------------------------------------------------------------
    silver_noc = transform_noc_regions(bronze_noc, noc_mapping=noc_mapping)


    silver_athletes.write.mode("overwrite").partitionBy("year").parquet(str(silver_path / "athlete_events"))

    silver_noc.write.mode("overwrite").parquet(
        str(silver_path / "noc_regions")
    )

    # Return the DataFrames so main.py can use them directly to build Gold
    # dimensions and the fact table during the same pipeline execution.
    return silver_athletes, silver_noc
