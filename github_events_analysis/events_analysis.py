"""This script is in charge of the whole flow producing the complete set
of results. It makes use of the `user_aggregation` and `repo_aggregation`
packages to extract the final metrics"""
from github_events_analysis.repo_aggregation.repo_aggs import (
    get_repo_aggregations,
)
from github_events_analysis.user_aggregation.user_aggs import (
    get_user_aggregations,
)
from github_events_analysis.utils.data import get_complete_dataset_from_dates
from github_events_analysis.utils.dates import (
    extract_date_from_created_at,
)
from github_events_analysis.utils.io import write


def main(
    initial_day: int = 1,
    last_day: int = 31,
) -> None:
    """Script in charge of the whole flow. No object is returned, but .csv
    files are written containing the results.

    Args:
        initial_day (int): Initial month to analyze. The default value is
            the first day of the month.
        last_day (int): End month to analyze. The default value is 31 (since
            the dataset is for January 2022).

    """
    data_to_use = get_complete_dataset_from_dates(
        initial_day=initial_day,
        last_day=last_day,
    )

    data_with_date = extract_date_from_created_at(
        dataset=data_to_use,
    )

    user_metrics = get_user_aggregations(
        data=data_with_date,
    )

    repo_metrics = get_repo_aggregations(
        data=data_with_date,
    )

    write(
        dataset=user_metrics,
        partition_column="day",
        path="/Users/rives4/Desktop/users"
    )
