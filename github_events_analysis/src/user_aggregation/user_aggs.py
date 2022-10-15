"""This script gathers all the functions needed to extract the user
aggregation metrics"""
from pyspark.sql.dataframe import DataFrame

from github_events_analysis.src.classes.events import Event
from github_events_analysis.src.utils.events import filter_by_event_type


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
    filtered_data = filter_by_event_type(
        dataset=data,
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
        metrics (DataFrame): Aggregated dataset with number of
            starred projects, created issues and created PRs
    """
    metrics = (
        dataset
        .groupBy(
            [
                "actor_login",
                "actor_id",
                "day",
                "type"
            ]
        )
        .count()
    )

    return metrics
