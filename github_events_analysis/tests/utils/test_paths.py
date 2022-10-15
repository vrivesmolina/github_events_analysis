from github_events_analysis.src.utils.paths import (
    correct_path,
)


def test_correct_path():
    assert correct_path("hello") == "hello/"
    assert correct_path("hello/") == "hello/"
