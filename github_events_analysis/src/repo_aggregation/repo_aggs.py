"""This script gathers all the functions needed to extract the repository
aggregation metrics"""
from pyspark.sql.dataframe import DataFrame

from github_events_analysis.src.classes.events import Event
from github_events_analysis.src.utils.events import filter_by_event_type
from github_events_analysis.src.utils.io import write
from github_events_analysis.src.utils.paths import correct_path


def get_repo_aggregations(
    data: DataFrame,
    output_path: str,
) -> None:
    """Main function in this flow, it creates the dataset of metrics for
    repository aggregation

    Args:
       data (DataFrame): Raw dataset from which extract the metrics
       output_path (str): Output path for repo-aggregated metrics

    """
    filtered_data = filter_by_event_type(
        dataset=data,
        events_to_keep=[Event.Issues, Event.PullRequest]
    )

    type_metrics = _get_repo_metrics(
        dataset=filtered_data,
        metric="type"
    )
    
    fork_metrics = _get_repo_metrics(
        dataset=filtered_data,
        metric="number_of_forks"
    )

    corrected_output_path = correct_path(
        path=output_path,
    )

    write(
        dataset=type_metrics,
        partition_column="day",
        path=corrected_output_path + "type/",
    )

    write(
        dataset=fork_metrics,
        partition_column="day",
        path=corrected_output_path + "forks/",
    )


def _get_repo_metrics(
    dataset: DataFrame,
    metric: str,
) -> DataFrame:
    """Get repo aggregation metrics: number of starred projects, created issues
    and created PRs per user-date

    Args:
        dataset (DataFrame): Dataset from where we will get the metrics
        metric (str): Metric to extract

    Return:
        metrics (DataFrame): Aggregated dataset with number of users that
            starred it, forked it and number of created issues and PRs

    """
    metrics = (
        dataset
        .groupBy(
            [
                "project_id",
                "project_name",
                "day",
                metric
            ]
        )
        .count()
    )

    return metrics
