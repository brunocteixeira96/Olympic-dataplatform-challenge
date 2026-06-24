from datetime import date
from pathlib import Path
import olympic_pipeline.basic as u

from olympic_pipeline.dimensions import (
    build_dim_athlete,
    build_dim_event,
    build_dim_games
)
from olympic_pipeline.facts import build_fact_participation
from olympic_pipeline.ingestion import ingest_bronze
from olympic_pipeline.quality_checks import (
    check_no_null_keys,
    check_not_empty,
    check_one_current_noc_version,
    check_noc_reference_coverage,
    check_foreign_key_integrity
)
from olympic_pipeline.scd_type_2 import (
    create_initial_noc_dimension,
    merge_noc_scd_type_2
)
from olympic_pipeline.silver import build_silver
from olympic_pipeline.spark_session import create_spark_session


def main():
    argument_keys = [
        "input_path",
        "reference_path",
        "output_path",
        "batch_date"
    ]

    u.validate_arguments(argument_keys)

    raw_args = u.get_args(argument_keys)

    input_path = Path(raw_args["input_path"])
    reference_path = Path(raw_args["reference_path"])
    output_path = Path(raw_args["output_path"])
    batch_date = date.fromisoformat(raw_args["batch_date"])

    u.validate_input_directory(input_path)

    spark = create_spark_session()

    bronze_path = output_path / "bronze"
    silver_path = output_path / "silver"
    gold_path = output_path / "gold"
    mapping_file = reference_path / "noc_code_mapping.csv"


    try:
        u.log("######################################################")
        u.log("Starting Olympic data pipeline")
        u.log("################### PARAMETERS #######################")
        u.log(f"## Input path: {input_path}")
        u.log(f"## Reference path: {reference_path}")
        u.log(f"## Output path: {output_path}")
        u.log(f"## Batch date: {batch_date}")
        u.log(f"## Mapping file: {mapping_file}")

        # -----------------------------------
        # 1. Bronze ingestion
        # -----------------------------------
        u.log("######################################################")
        u.log("################ Building Bronze layer ###############")
        u.log("######################################################")

        ingest_bronze(
            spark=spark,
            input_path=input_path,
            reference_path=reference_path,
            bronze_path=bronze_path,
            batch_date=batch_date
        )


        # -----------------------------------
        # 2. Silver transformations
        # -----------------------------------
        u.log("######################################################")
        u.log("################ Building Silver layer ###############")
        u.log("######################################################")

        silver_athletes, silver_noc = build_silver(
            spark=spark,
            bronze_path=bronze_path,
            silver_path=silver_path,
            batch_date=batch_date
        )

        # -----------------------------------
        ## 2.1 Silver data quality checks
        # -----------------------------------      


        check_noc_reference_coverage(
            silver_athletes=silver_athletes,
            silver_noc=silver_noc,
        )

        check_not_empty(
            df=silver_athletes,
            table_name="silver_athlete_events",
        )

        check_not_empty(
            df=silver_noc,
            table_name="silver_noc_regions",
        )

        check_no_null_keys(
            df=silver_athletes,
            key_columns=["athlete_id"],
            table_name="silver_athlete_events",
        )

        check_no_null_keys(
            df=silver_noc,
            key_columns=["noc_code"],
            table_name="silver_noc_regions",
        )


        # -----------------------------------
        # 3. Regular Gold dimensions
        # -----------------------------------
        u.log("######################################################")
        u.log("############## Building Gold dimensions ##############")
        u.log("######################################################")

        dim_athlete = build_dim_athlete(
            athlete_events=silver_athletes
        )

        dim_games = build_dim_games(
            athlete_events=silver_athletes
        )

        dim_event = build_dim_event(
            athlete_events=silver_athletes
        )

        # -----------------------------------
        ## 3.1 Gold dims data quality checks
        # -----------------------------------  

        check_not_empty(dim_athlete, "dim_athlete")
        check_not_empty(dim_games, "dim_games")
        check_not_empty(dim_event, "dim_event")

        # -----------------------------------
        # 4. SCD Type 2 NOC dimension
        # -----------------------------------
        dim_noc_path = gold_path / "dim_noc"
        u.log("------------------------------------------------------")
        if dim_noc_path.exists():
            u.log("Updating existing dim_noc with SCD Type 2...")

            existing_dim_noc = spark.read.parquet(
                str(dim_noc_path)
            )

            dim_noc = merge_noc_scd_type_2(
                existing=existing_dim_noc,
                incoming=silver_noc,
                batch_date=batch_date,
            )
        else:
            u.log("Creating initial dim_noc...")

            dim_noc = create_initial_noc_dimension(
                incoming=silver_noc,
                batch_date=batch_date,
            )
        
        # persisting and materializing dim_noc
        dim_noc.persist()
        dim_noc.count()

        # -----------------------------------
        ## 4.1 Gold dims data quality checks
        # -----------------------------------  

        check_not_empty(dim_noc, "dim_noc")
        check_one_current_noc_version(dim_noc)
        u.log("------------------------------------------------------")

        # -----------------------------------
        # 5. Gold fact table
        # -----------------------------------
        u.log("Building fact_participation...")

        fact_participation = build_fact_participation(
            athlete_events=silver_athletes,
            dim_athlete=dim_athlete,
            dim_games=dim_games,
            dim_event=dim_event,
            dim_noc=dim_noc,
        )

        # -----------------------------------
        # 5.1 Gold fact table data quality checks
        # -----------------------------------

        check_not_empty(
            df=fact_participation,
            table_name="fact_participation",
        )

        check_no_null_keys(
            df=fact_participation,
            key_columns=[
                "athlete_key",
                "games_key",
                "event_key",
                "noc_key",
            ],
            table_name="fact_participation",
        )

        check_foreign_key_integrity(
            fact_df=fact_participation,
            dimension_df=dim_athlete,
            key_column="athlete_key",
            dimension_name="dim_athlete",
            fact_name='fact_participation'
        )

        check_foreign_key_integrity(
            fact_df=fact_participation,
            dimension_df=dim_games,
            key_column="games_key",
            dimension_name="dim_games",
            fact_name='fact_participation'
        )

        check_foreign_key_integrity(
            fact_df=fact_participation,
            dimension_df=dim_event,
            key_column="event_key",
            dimension_name="dim_event",
            fact_name='fact_participation'
        )

        check_foreign_key_integrity(
            fact_df=fact_participation,
            dimension_df=dim_noc,
            key_column="noc_key",
            dimension_name="dim_noc",
            fact_name='fact_participation'
        )
        u.log("------------------------------------------------------")

        # -----------------------------------
        # 7. Write Gold layer
        # -----------------------------------
        u.log("******************************************************")
        u.log("***************** Writing Gold layer *****************")
        u.log("******************************************************")
        
        try:
            dim_athlete.write.mode("overwrite").parquet(
                str(gold_path / "dim_athlete")
            )
            u.log("****** Data successfuly written to dim_athlete! ******")

            dim_games.write.mode("overwrite").parquet(
                str(gold_path / "dim_games")
            )
            u.log("******** Data successfuly written to dim_games! ******")

            dim_event.write.mode("overwrite").parquet(
                str(gold_path / "dim_event")
            )
            u.log("******** Data successfuly written to dim_event! ******")

            fact_participation.write.mode("overwrite").partitionBy("year").parquet(
                str(gold_path / "fact_participation")
            )
            u.log("** Data successfuly written to fact_participation! ***")

            dim_noc.write.mode("overwrite").parquet(
                str(dim_noc_path)
            )
            dim_noc.unpersist()
            u.log("********** Data successfuly written to dim_noc! ******")

        except Exception as error:
            u.log(f"** Gold layer write failed : {error}")
            raise

        u.log("******************************************************")
        u.log("******************************************************")
        u.log("********** Pipeline completed successfully************")
        u.log(f"************* for date : {batch_date} ***************")
        u.log("******************************************************")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()