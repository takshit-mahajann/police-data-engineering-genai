from pyspark.sql import DataFrame
from pyspark import StorageLevel


def persist_data(
    df: DataFrame,
    storage_level=StorageLevel.MEMORY_AND_DISK
) -> DataFrame:

    return df.persist(storage_level)


def unpersist_data(
    df: DataFrame
) -> DataFrame:

    df.unpersist()

    return df