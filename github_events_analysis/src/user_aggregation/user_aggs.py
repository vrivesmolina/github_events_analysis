"""This script gathers all the functions needed to extract the user
aggregation metrics"""
from pyspark.sql.dataframe import DataFrame

from github_events_analysis.src.classes.events import Event
from github_events_analysis.src.utils.events import filter_by_event_type
from github_events_analysis.src.utils.io import write
from github_events_analysis.src.utils.paths import correct_path


def get_user_aggregations(
    data: DataFrame,
    output_path: str,
) -> None:
    """Main function in this flow, it creates the dataset of metrics for
    user aggregation

    Args:
       data (DataFrame): Raw dataset from which extract the metrics
       output_path (str): Output path for user-aggregated metrics

    """
    filtered_data = filter_by_event_type(
        dataset=data,
        events_to_keep=[Event.Issues, Event.PullRequest]
    )

    type_metrics = _get_user_metrics(
        dataset=filtered_data
    )

    correct_output_path = correct_path(
        path=output_path,
    )

    write(
        dataset=type_metrics,
        partition_column="day",
        path=correct_output_path,
    )


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
