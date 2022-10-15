""" This script gathers all the utilities needed to work with Spark """
from pyspark.sql import SparkSession


def get_spark_session():
    """Get Spark Session

    Return:
        spark (SparkSession): SparkSession for this project

    """
    spark = (
        SparkSession
        .builder
        .appName("github_events_analysis")
        .getOrCreate()
    )

    return spark
