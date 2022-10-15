"""Script that gathers all the functions that transform a dataset by event
information for user aggregations"""
from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col


def filter_by_event_type(
    dataset: DataFrame,
    events_to_keep: List[str],
) -> DataFrame:
    """Filter the dataset, only keeping the events in the input list

    Args:
        dataset (DataFrame): Dataframe to filter
        events_to_keep (List[str]): List of events to keep

    Return:
        filtered_dataset (DataFrame): Filtered dataframe with the same columns
            as the input dataframe

    """
    filtered_dataset = (
        dataset
        .filter(col("type").isin(events_to_keep))
    )

    return filtered_dataset
