"""Script with all utility functions for paths"""


def correct_path(
    path: str,
) -> str:
    """Make sure the path has a `/` at the end

    Args:
        path (str): Path to correct

    Return:
        corrected_path (str): Input path, making sure there is a '/' at the
            end of it

    """
    corrected_path = (
        path
        if path.endswith("/")
        else
        path + "/"
    )

    return corrected_path
