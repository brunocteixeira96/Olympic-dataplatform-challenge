from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
# Bronze schemas
# ---------------------------------------------------------------------------
# Bronze is the raw ingestion layer. All source columns are initially read as
# strings so that Spark preserves the original values exactly as received.
#
# Data-type conversions, such as converting ID to long or Year to integer,
# are performed later in the Silver layer. This prevents malformed values
# from causing the entire CSV ingestion to fail and makes rejected values
# easier to investigate.

# Schema for the source athlete_events.csv dataset.
ATHLETE_EVENTS_BRONZE_SCHEMA = StructType(
    [
        # Athlete identifier supplied by the source.
        # It remains a string in Bronze and is converted to long in Silver.
        StructField("ID", StringType(), True),

        # Descriptive athlete attributes.
        StructField("Name", StringType(), True),
        StructField("Sex", StringType(), True),
        StructField("Age", StringType(), True),
        StructField("Height", StringType(), True),
        StructField("Weight", StringType(), True),

        # Team and National Olympic Committee information.
        StructField("Team", StringType(), True),
        StructField("NOC", StringType(), True),

        # Olympic Games information.
        StructField("Games", StringType(), True),
        StructField("Year", StringType(), True),
        StructField("Season", StringType(), True),
        StructField("City", StringType(), True),

        # Sport, event and medal information.
        StructField("Sport", StringType(), True),
        StructField("Event", StringType(), True),
        StructField("Medal", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ]
)

# Schema for the source noc_regions.csv reference dataset.
NOC_REGIONS_BRONZE_SCHEMA = StructType(
    [
        # NOC code supplied by the source, for example POR, USA or SIN.
        StructField("NOC", StringType(), True),
        # Country or region associated with the NOC code.
        StructField("region", StringType(), True),
        StructField("notes", StringType(), True),
        # Stores malformed CSV records for later investigation.
        StructField("_corrupt_record", StringType(), True)
    ]
)

# Schema for the governed NOC-code mapping reference dataset.
#
# This mapping resolves situations where different source systems use
# different codes for the same business entity.
#
# Example:
#
#   athlete_events uses: SGP
#   noc_regions uses:    SIN
#
# Both codes represent Singapore. Instead of modifying the original source
# files or hardcoding a special rule inside the silver layer, the mapping file
# standardizes both sources to one approved canonical code:
#
#   noc_regions | SIN | SGP
#
# The Bronze layer preserves this mapping file exactly as it was supplied.
# The mapping is then applied in the Silver layer before dimensions and facts are created.
NOC_CODE_MAPPING_BRONZE_SCHEMA = StructType([
    StructField("source_system", StringType(), False),
    StructField("source_noc_code", StringType(), False),
    StructField("canonical_noc_code", StringType(), False),
    StructField("reason", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])