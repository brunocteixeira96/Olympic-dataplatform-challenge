from pathlib import Path
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType
from olympic_pipeline.quality_checks import check_count_rows


from olympic_pipeline.schemas import (
    ATHLETE_EVENTS_BRONZE_SCHEMA,
    NOC_CODE_MAPPING_BRONZE_SCHEMA,
    NOC_REGIONS_BRONZE_SCHEMA,
)


def read_csv(spark: SparkSession, path: Path, schema: StructType):
    """
    Read a CSV file using an explicitly defined Bronze schema.

    Parameters:
    spark: Active Spark session used to read the CSV file.
    path: Full path to the source CSV file.
    schema: Expected Spark schema for the source dataset.

    Returns:
    DataFrame :Raw DataFrame containing the source columns, including the optional _corrupt_record column.
    """

    return (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record",
        )
        .option("enforceSchema", False)
        .option("delimiter", ",")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .csv(str(path))
    )


def add_ingestion_metadata(df: DataFrame, source_filename: str, batch_date: date):
    """
    Add technical metadata to an ingested Bronze DataFrame.
    """

    return (
        df
        .withColumn("_ingested_at", f.current_timestamp())
        .withColumn("_source_filename", f.lit(source_filename))
        .withColumn("_batch_date", f.lit(batch_date).cast("date"))
    )


def ingest_bronze(spark: SparkSession, input_path: Path, reference_path: Path, bronze_path: Path, batch_date: date):
    """
    Ingest the operational source files and governed reference data into
    the Bronze layer.

    Parameters:
    spark: Active Spark session.
    input_path: Directory containing the source operational input files: athlete_events.csv and noc_regions.csv
    reference_path: Directory containing governed platform reference data, such as noc_code_mapping.csv.
    bronze_path: Root output directory for the Bronze Parquet datasets.
    """

    # ------------------------------------------------------------------
    # 1. Ingest athlete_events.csv
    # ------------------------------------------------------------------
    # This is the main source dataset containing one row per athlete participation in an Olympic event.
    athlete_events = read_csv(spark=spark, path=input_path / "athlete_events.csv", schema=ATHLETE_EVENTS_BRONZE_SCHEMA)

    # Add technical metadata without changing the original source columns.
    athlete_events = add_ingestion_metadata(df=athlete_events, source_filename="athlete_events.csv", batch_date=batch_date)

    # Write the raw athlete data to the Bronze layer in Parquet format.
    athlete_events.write.mode("overwrite").partitionBy("_batch_date").parquet(str(bronze_path / "athlete_events"))

    # ------------------------------------------------------------------
    # 2. Ingest noc_regions.csv
    # ------------------------------------------------------------------
    # This is the source reference dataset that associates an NOC code with a country or region.
    noc_regions = read_csv(spark=spark, path=input_path / "noc_regions.csv", schema=NOC_REGIONS_BRONZE_SCHEMA)

    noc_regions = add_ingestion_metadata(df=noc_regions, source_filename="noc_regions.csv", batch_date=batch_date)

    noc_regions.write.mode("overwrite").partitionBy("_batch_date").parquet(str(bronze_path / "noc_regions"))

    # ------------------------------------------------------------------
    # 3. Ingest noc_code_mapping.csv
    # ------------------------------------------------------------------
    # This is governed platform reference data rather than an original
    # source input dataset.
    #
    # It resolves cases where two source datasets use different codes for the same business entity.
    #
    # Example:
    #
    #   athlete_events uses SGP for Singapore
    #   noc_regions uses SIN for Singapore
    #
    # The mapping standardizes SIN to the approved code SGP.
    #
    # The mapping is ingested into Bronze so that each pipeline execution
    # preserves the exact version of the reference data used by that batch.
    noc_code_mapping = read_csv( spark=spark, path=reference_path / "noc_code_mapping.csv", schema=NOC_CODE_MAPPING_BRONZE_SCHEMA)

    noc_code_mapping = add_ingestion_metadata(df=noc_code_mapping, source_filename="noc_code_mapping.csv", batch_date=batch_date)

    # Reference data is placed under a dedicated Bronze subdirectory to
    # distinguish governed mappings from operational source datasets.
    noc_code_mapping.write.mode("overwrite").partitionBy("_batch_date").parquet(str(bronze_path / "reference" / "noc_code_mapping"))

    # ------------------------------------------------------------------
    # 4. Log ingestion row counts
    # ------------------------------------------------------------------
    # These counts provide a simple ingestion audit and can later be compared
    # against Silver and Gold row counts.

    check_count_rows(athlete_events, "athlete_events.csv")
    check_count_rows(noc_regions, "noc_regions.csv")
    check_count_rows(noc_code_mapping, "noc_code_mapping.csv")

