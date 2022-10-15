"""This script gathers all the utility functions related to data and their
transformations"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from github_events_analysis.src.exceptions.dates import (
    NotValidDatesException
)
from github_events_analysis.src.utils.spark import get_spark_session


def get_complete_dataset_from_dates(
    data_path: str,
    initial_day: int,
    last_day: int,
) -> DataFrame:
    """Get dataset for the input dates

    Args:
        data_path (str): Path where the data are located
        initial_day (int): First day to get the corresponding data
        last_day (int): Last day to get the corresponding data

    Returns:
        dataset (DataFrame): Raw dataset for the input dates

    """
    if last_day < initial_day:
        raise NotValidDatesException(
            initial_day=initial_day,
            last_day=last_day,
        )

    spark = get_spark_session()

    data = {}
    for day in range(initial_day, last_day + 1):
        day_str = get_two_digit_str(an_int=day)
        full_day_data = spark.read.json(
            f"{data_path}/day_{day_str}"
        )
        data[day] = full_day_data.select(
            col("type"),
            col("created_at"),
            col("actor.login").alias("actor_login"),
            col("actor.id").alias("actor_id"),
            col("repo.id").alias("project_id"),
            col("repo.name").alias("project_name"),
            col("payload.forkee.forks_count").alias("number_of_forks")
        )

    all_data = data[list(data.keys())[0]]
    for day in range(initial_day + 1, last_day + 1):
        all_data = all_data.union(data[day])

    return all_data


def get_two_digit_str(
    an_int: int,
) -> str:
    """Transform an int into a string to fulfill the format of the folders
        to read

    Args:
        an_int (int): Int to transform

    Return:
        a_string (str): String representation of the int, with two digits

    """
    a_string = (
        f"0{an_int}"
        if an_int < 10
        else f"{an_int}"
    )

    return a_string
