"""This script is in charge of the whole flow producing the complete set
of results. It makes use of the `user_aggregation` and `repo_aggregation`
modules to extract the final metrics"""

import webbrowser

from github_events_analysis.src.repo_aggregation.repo_aggs import (
    get_repo_aggregations,
)
from github_events_analysis.src.user_aggregation.user_aggs import (
    get_user_aggregations,
)
from github_events_analysis.src.utils.data import (
    get_complete_dataset_from_dates,
    get_two_digit_str,
)
from github_events_analysis.src.utils.dates import (
    extract_date_from_created_at,
)


def get_files(
    initial_day: int = 1,
    final_day: int = 31,
) -> None:
    """Download the files you use for the analysis

    Args:
        initial_day (int): Initial day for the data to download
        final_day (int): Final day for the data to download

    """
    for day in range(initial_day, final_day + 1):
        for hour in range(0, 24):
            hour_str = get_two_digit_str(an_int=hour)
            day_str = get_two_digit_str(an_int=day)
            url = (
                f"https://data.gharchive.org/"
                f"2022-01-{day_str}-{hour_str}"
                f".json.gz"
            )
            webbrowser.open(url=url)


def main(
    data_path: str,
    repository_output_path: str,
    user_output_path: str,
    initial_day: int = 1,
    last_day: int = 31,
) -> None:
    """Script in charge of the whole flow. No object is returned, but .csv
    files are written containing the results.

    Args:
        data_path (str): Input path to the data. The data must be in the form
             of day_XX under this path
        user_output_path (str): Output path for user-aggregated metrics
        repository_output_path (str): Output path for repository-aggregated
            metrics
        initial_day (int): Initial month to analyze. The default value is
            the first day of the month.
        last_day (int): End month to analyze. The default value is 31 (since
            the dataset is for January 2022).

    """
    data_to_use = get_complete_dataset_from_dates(
        data_path=data_path,
        initial_day=initial_day,
        last_day=last_day,
    )

    data_with_date = extract_date_from_created_at(
        dataset=data_to_use,
    )

    get_user_aggregations(
        data=data_with_date,
        output_path=user_output_path,
    )

    get_repo_aggregations(
        data=data_with_date,
        output_path=repository_output_path,
    )
