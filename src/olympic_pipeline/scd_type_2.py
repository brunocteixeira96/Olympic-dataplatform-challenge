from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as f

MAX_DATE = "9999-12-31"

def create_initial_noc_dimension(incoming: DataFrame, batch_date: date):
    return (
        incoming.withColumn(
            "valid_from",
            f.lit(batch_date.isoformat()).cast("date")
        )
        .withColumn(
            "valid_to",
            f.lit(MAX_DATE).cast("date")
        )
        .withColumn("is_current", f.lit(True))
        .withColumn(
            "noc_key",
            f.xxhash64(
                "noc_code",
                "record_hash",
                "valid_from",
            ),
        )
        .select(
            "noc_key",
            "noc_code",
            "region",
            "notes",
            "record_hash",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )

def merge_noc_scd_type_2(existing: DataFrame, incoming: DataFrame, batch_date: date):
    current = existing.filter(f.col("is_current"))

    comparison = (
        incoming.alias("src")
        .join(
            current.alias("tgt"),
            on="noc_code",
            how="left"
        )
        .select(
            f.col("src.noc_code"),
            f.col("src.region"),
            f.col("src.notes"),
            f.col("src.record_hash"),
            f.col("tgt.record_hash").alias("existing_hash"),
        )
        .withColumn(
            "change_type",
            f.when(
                f.col("existing_hash").isNull(),
                f.lit("INSERT")
            )
            .when(
                f.col("record_hash") != f.col("existing_hash"),
                f.lit("UPDATE")
            )
            .otherwise(f.lit("NO_CHANGE"))
        )
    )

    changed_codes = (
        comparison
        .filter(f.col("change_type") == "UPDATE")
        .select("noc_code")
    )

    expired_existing = (
        existing.alias("dim")
        .join(
            changed_codes.alias("changes"),
            on="noc_code",
            how="left"
        )
        .withColumn(
            "valid_to",
            f.when(
                f.col("changes.noc_code").isNotNull() & f.col("dim.is_current"),
                f.date_sub(
                    f.lit(batch_date.isoformat()).cast("date"),
                    1,
                ),
            ).otherwise(f.col("dim.valid_to"))
        )
        .withColumn(
            "is_current",
            f.when(
                f.col("changes.noc_code").isNotNull() & f.col("dim.is_current"),
                f.lit(False)
            ).otherwise(f.col("dim.is_current"))
        )
        .select("dim.*", "valid_to", "is_current")
        .drop("dim.valid_to", "dim.is_current")
    )

    new_versions = (
        comparison
        .filter(f.col("change_type").isin("INSERT", "UPDATE"))
        .drop("existing_hash", "change_type")
        .withColumn(
            "valid_from",
            f.lit(batch_date.isoformat()).cast("date"),
        )
        .withColumn(
            "valid_to",
            f.lit(MAX_DATE).cast("date"),
        )
        .withColumn("is_current", f.lit(True))
        .withColumn(
            "noc_key",
            f.xxhash64(
                "noc_code",
                "record_hash",
                "valid_from"
            )
        )
        .select(
            "noc_key",
            "noc_code",
            "region",
            "notes",
            "record_hash",
            "valid_from",
            "valid_to",
            "is_current"
        )
    )

    return expired_existing.unionByName(new_versions)