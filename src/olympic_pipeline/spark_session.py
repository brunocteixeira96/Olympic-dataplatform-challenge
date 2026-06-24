from pyspark.sql import SparkSession

def create_spark_session():
    """Function needed to create a Spark session"""

    return (
        SparkSession.builder
        .appName("olympic-data-platform")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config( "spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )