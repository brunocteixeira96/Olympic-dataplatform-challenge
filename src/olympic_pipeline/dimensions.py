from pyspark.sql import DataFrame
from pyspark.sql import functions as f

def build_dim_athlete(athlete_events: DataFrame):
    """
    Build the Gold athlete dimension from the Silver athlete-events dataset.

    Parameters: 
    athlete_events:
        Silver athlete-events DataFrame.

        It contains one row per athlete participation, so the same athlete may
        appear several times across different Olympic Games and events.

        The columns used by this dimension are:

        - athlete_id: source business identifier for the athlete;
        - athlete_name: descriptive athlete name;
        - sex: standardized athlete sex.

    Returns:
    DataFrame
        Athlete dimension containing one row per athlete with:

        - athlete_key: generated surrogate key;
        - athlete_id: source business key;
        - athlete_name;
        - sex.
    """    
    return (
        athlete_events.select(
            "athlete_id",
            "athlete_name",
            "sex"
        )
        .dropDuplicates(["athlete_id"])
        .withColumn(
            "athlete_key",
            f.xxhash64("athlete_id")
        )
        .select(
            "athlete_key",
            "athlete_id",
            "athlete_name",
            "sex"
        )
    )


def build_dim_games(athlete_events: DataFrame):
    """
    Build the Gold Olympic Games dimension.

    Parameters: 
    athlete_events:
        Silver athlete-events DataFrame.

        The same Olympic Games edition appears in many participation records.
        The columns used to construct this dimension are:

        - games_name: business identifier of the Games edition;
        - year: Olympic year;
        - season: Summer or Winter;
        - host_city: city where the Games were hosted.

    Returns:
    DataFrame
        Games dimension containing one row per Olympic Games edition with:

        - games_key: generated surrogate key;
        - games_name: dimension business key;
        - year;
        - season;
        - host_city.
    """
    return (
        athlete_events.select(
            "games_name",
            "year",
            "season",
            "host_city",
        )
        .dropDuplicates(["games_name"])
        .withColumn(
            "games_key",
            f.xxhash64("games_name")
        )
        .select(
            "games_key",
            "games_name",
            "year",
            "season",
            "host_city"
        )
    )


def build_dim_event(athlete_events: DataFrame):
    """
    Build the Gold Olympic event dimension.

    Parameters:
    athlete_events:
        Silver athlete-events DataFrame.

        The same event appears in many athlete participation records.

        The columns used to identify an event are:

        - sport: general sporting discipline;
        - event_name: specific Olympic event.

    Returns:
    DataFrame
        Event dimension containing one row per unique sport and event
        combination with:

        - event_key: generated surrogate key;
        - sport;
        - event_name.
    """
    return (
        athlete_events.select(
            "sport",
            "event_name"
        )
        .dropDuplicates(["sport", "event_name"])
        .withColumn(
            "event_key",
            f.xxhash64("sport", "event_name")
        )
        .select(
            "event_key",
            "sport",
            "event_name"
        )
    )