import pytest

from github_events_analysis.src.exceptions.dates import NotValidDatesException
from github_events_analysis.src.utils.data import (
    get_two_digit_str,
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
    assert get_two_digit_str(an_int=8) == "08"
    assert get_two_digit_str(an_int=19) == "19"
