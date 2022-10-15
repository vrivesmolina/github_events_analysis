"""Script that gathers the functions used for I/O files"""
from pyspark.sql.dataframe import DataFrame


def write(
    dataset: DataFrame,
    path: str,
    partition_column: str = None,
) -> None:
    """Writes dataframe in the given path, partitioned, as CSV

    Args:
        dataset (DataFrame): Dataset to write
        path (str): Path where the dataframe will be written
        partition_column (str): Partition column (optional)

    """
    if partition_column:
        (
            dataset
            .write
            .mode("overwrite")
            .partitionBy(partition_column)
            .csv(path=path)
        )

    else:
        dataset.write.mode("overwrite").csv(path=path)
