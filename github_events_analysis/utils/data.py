"""This script gathers all the utility functions related to data and their
transformations"""
from pyspark.sql.dataframe import DataFrame

from github_events_analysis.exceptions.dates import (
    NotValidDatesException
)
from github_events_analysis.utils.spark import get_spark_session


def get_complete_dataset_from_dates(
    initial_day: int,
    last_day: int,
) -> DataFrame:
    """Get dataset for the input dates

    Args:
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
        day_str = _get_day_str(day=day)
        full_day_data = spark.read.json(
            f"/Users/rives4/Desktop/schneider/day_{day_str}"
        )
        data[day] = full_day_data.select(
            "type",
            "created_at",
            "actor.login"
        )

    all_data = data[list(data.keys())[0]]
    for day in range(initial_day + 1, last_day + 1):
        all_data = all_data.union(data[day])

    return all_data


def _get_day_str(
    day: int,
) -> str:
    """Transform the day as an int to a day as a string to fulfill the format
    of the folders to read

    Args:
        day (int): Day to transform from int to str

    Return:
        day_string (str): Day as string, with two digits

    """
    day_string = (
            f"0{day}"
            if day < 10
            else f"{day}"
        )

    return day_string
