"""This script gathers all the functions needed to extract the repository
aggregation metrics"""
from pyspark.sql.dataframe import DataFrame


def get_repo_aggregations(
    data: DataFrame,
) -> DataFrame:
    """Main function in this flow, it creates the dataset of metrics for
    repository aggregation

    Args:
       data (DataFrame): Raw dataset from which extract the metrics

    Return:
        metrics (DataFrame): Metrics related to repository aggregation

    """
    return data
