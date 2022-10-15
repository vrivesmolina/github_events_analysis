"""Script that gathers all the functions needed for the transformations
required on times and dates columns for the user aggregations"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import (
    col,
    substring_index,
)


def extract_date_from_created_at(
    dataset: DataFrame,
) -> DataFrame:
    """Get the date from the `created_at` column. The format of this column is
    YYYY-MM-ddTHH:mm:ssZ, so we can use `substring_index` to get the date

    Args:
        dataset (DataFrame): DataFrame with the `created_at` column

    Return:
        dataset_with_date (DataFrame): Input dataframe with an additional
            column, corresponding to the day

    """
    dataset_with_date = (
        dataset
        .withColumn(
            "day",
            substring_index(
                col("created_at"),
                "T",
                1
            )
        )
    )

    return dataset_with_date
