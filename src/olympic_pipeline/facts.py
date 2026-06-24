from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def build_fact_participation(athlete_events: DataFrame, dim_athlete: DataFrame, dim_games: DataFrame, dim_event: DataFrame, dim_noc: DataFrame):
    """
    Build the Gold participation fact table.

    Grain
    -----
    One athlete participating in one event, at one Olympic Games edition,
    representing one team and NOC.

    Parameters:
    athlete_events:
        Silver athlete-events DataFrame.

        This is the main source DataFrame used to create the fact table. It
        contains one row per athlete participation.
        The business identifiers in this DataFrame are used to look up the
        surrogate keys stored in the Gold dimensions.

    dim_athlete:
        Gold athlete dimension.

        It contains one record per athlete and provides athlete_key, the
        surrogate key that is stored in the fact table.

        The join is performed using athlete_id.

    dim_games:
        Gold Olympic Games dimension.

        It contains one record per Olympic Games edition and provides
        games_key, the surrogate key stored in the fact table.

        The join is performed using games_name.

    dim_event:
        Gold event dimension.

        It contains one record per sport and event combination and provides
        event_key, the surrogate key stored in the fact table.

        The join is performed using both sport and event_name.

    dim_noc:
        Gold NOC dimension implemented as SCD Type 2.

        It may contain several historical versions of the same noc_code.
        Only the current version is used when building this fact table.

        The dimension provides noc_key, the surrogate key stored in the fact.

        The join is performed using the canonical noc_code produced in the
        Silver layer.

    Returns:
    DataFrame
        Gold participation fact DataFrame containing:

        - the deterministic participation key;
        - dimension surrogate keys;
        - Olympic year;
        - participation-level attributes;
        - additive participation and medal counters.
    """

    # The NOC dimension is implemented as SCD Type 2 and may therefore contain
    # several historical versions of the same noc_code.
    current_noc = dim_noc.filter(f.col("is_current"))

    return (
        athlete_events.alias("src")
        .join(
            dim_athlete.alias("athlete"),
            on="athlete_id",
            how="left"
        )
        .join(
            dim_games.alias("games"),
            on="games_name",
            how="left"
        )
        .join(
            dim_event.alias("event"),
            on=["sport", "event_name"],
            how="left"
        )
        .join(
            current_noc.alias("noc"),
            on="noc_code",
            how="left"
        )
        .select(
            f.col("src.source_record_hash").alias(
                "participation_key"
            ),
            f.col("athlete.athlete_key"),
            f.col("games.games_key"),
            f.col("event.event_key"),
            f.col("noc.noc_key"),
            f.col("src.year").alias("year"),
            f.col("src.team"),
            f.col("src.age"),
            f.col("src.height_cm"),
            f.col("src.weight_kg"),
            f.col("src.medal"),
            f.lit(1).alias("participation_count"),
            f.when(
                f.col("src.medal").isNotNull(),
                f.lit(1),
            )
            .otherwise(f.lit(0))
            .alias("medal_count"),
            f.when(
                f.col("src.medal") == "Gold",
                f.lit(1),
            )
            .otherwise(f.lit(0))
            .alias("gold_count"),
            f.when(
                f.col("src.medal") == "Silver",
                f.lit(1),
            )
            .otherwise(f.lit(0))
            .alias("silver_count"),
            f.when(
                f.col("src.medal") == "Bronze",
                f.lit(1),
            )
            .otherwise(f.lit(0))
            .alias("bronze_count")
        )
    )