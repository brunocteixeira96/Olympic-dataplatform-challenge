from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def clean_noc_mapping(mapping: DataFrame):
    """
    Clean and standardize the NOC-code mapping reference dataset.

    Parameters: 
    mapping: Bronze NOC mapping DataFrame.

    Returns:
    DataFrame
        Cleaned mapping DataFrame with standardized source-system names
        and uppercase NOC codes.

    Notes
    -----
    The mapping is cleaned before being applied so that differences in
    capitalization or surrounding whitespace do not prevent valid matches.

    For example:

        " NOC_REGIONS " becomes "noc_regions"
        " sin "         becomes "SIN"
        " sgp "         becomes "SGP"
    """
    return (
        mapping
        .select(
            f.lower(
                f.trim(f.col("source_system"))
            ).alias("source_system"),

            f.upper(
                f.trim(f.col("source_noc_code"))
            ).alias("source_noc_code"),

            f.upper(
                f.trim(f.col("canonical_noc_code"))
            ).alias("canonical_noc_code"),

            f.trim(
                f.col("reason")
            ).alias("reason"),
        )
        .filter(
            f.col("source_system").isNotNull()
        )
        .filter(
            f.col("source_noc_code").isNotNull()
        )
        .filter(
            f.col("canonical_noc_code").isNotNull()
        )
    )


def apply_noc_mapping(df: DataFrame, mapping: DataFrame, source_system: str):
    """
    Apply source-specific NOC-code mappings to a DataFrame.

    Parameters:
    df:
        DataFrame whose NOC codes must be standardized.
    mapping:
        Cleaned NOC mapping DataFrame produced by clean_noc_mapping().

    source_system:
        Name of the source currently being processed.
        Examples:
            "athlete_events"
            "noc_regions"

    Returns:
    DataFrame
        Original DataFrame with a canonical noc_code column added.


    Mapping behavior
    ----------------
    When a mapping exists:

        source_noc_code = SIN
        noc_code        = SGP

    When no mapping exists:

        source_noc_code = POR
        noc_code        = POR

    This allows the mapping file to contain only exceptions rather than
    requiring one row for every valid NOC code.
    """
    source_mapping = (
        mapping
        .filter( f.col("source_system") == f.lit(source_system.lower()))
        .select(
            "source_noc_code",
            "canonical_noc_code",
        )
    )

    return (
        df
        .join(
            source_mapping,
            on="source_noc_code",
            how="left",
        )
        .withColumn(
            "noc_code",
            f.coalesce(
                f.col("canonical_noc_code"),
                f.col("source_noc_code"),
            )
        )
        .drop("canonical_noc_code")
    )