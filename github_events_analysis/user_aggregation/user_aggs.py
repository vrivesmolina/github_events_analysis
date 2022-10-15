""" This script gathers all the functions needed to extract the user
aggregation metrics"""
from pyspark.sql.dataframe import DataFrame

from github_events_analysis.classes.events import Event
from github_events_analysis.user_aggregation.dates import (
    extract_date_from_created_at,
)
from github_events_analysis.user_aggregation.events import filter_by_event_type


def get_user_aggregations(
    data: DataFrame,
) -> DataFrame:
    """Main function in this flow, it creates the dataset of metrics for
    user aggregation

    Args:
       data (DataFrame): Raw dataset from which extract the metrics

    Return:
        metrics (DataFrame): Metrics related to user aggregation

    """
    data_with_date = extract_date_from_created_at(
        dataset=data,
    )

    filtered_data = filter_by_event_type(
        dataset=data_with_date,
        events_to_keep=[Event.Issues, Event.PullRequest]
    )

    metrics = _get_user_metrics(
        dataset=filtered_data
    )

    return metrics


def _get_user_metrics(
    dataset: DataFrame,
) -> DataFrame:
    """Get user aggregation metrics: number of starred projects, created issues
    and created PRs per user-date

    Args:
        dataset (DataFrame): Dataset from where we will get the metrics

    Return:
        metrics (DataFrame): Aggregated dataset by user-date with number of
            starred projects, created issues and created PRs
    """
    metrics = (
        dataset
        .groupBy(
            [
                "login",
                "day",
                "type"
            ]
        )
        .count()
    )

    return metrics
