import pytest

from github_events_analysis.src.exceptions.dates import NotValidDatesException
from github_events_analysis.src.utils.data import (
    _get_day_str,
    get_complete_dataset_from_dates,
)


def test_get_complete_dataset_from_dates_exception():
    with pytest.raises(NotValidDatesException):
        get_complete_dataset_from_dates(
            data_path="FOO",
            initial_day=8,
            last_day=7,
        )


def test__get_day_str():
    assert _get_day_str(day=8) == "08"
    assert _get_day_str(day=19) == "19"
