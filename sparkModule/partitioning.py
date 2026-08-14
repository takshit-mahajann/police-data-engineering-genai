from pyspark.sql import DataFrame


# ============================================================
# GET PARTITION COUNT
# ============================================================

def get_partition_count(
    df: DataFrame
) -> int:
    """
    Return the current number of partitions
    in the DataFrame.
    """

    return df.rdd.getNumPartitions()


# ============================================================
# REPARTITION
# ============================================================

def repartition_data(
    df: DataFrame,
    num_partitions: int
) -> DataFrame:
    """
    Redistribute data into the requested number
    of partitions.

    This causes a shuffle.
    """

    if num_partitions <= 0:
        raise ValueError(
            "Number of partitions must be greater than 0."
        )

    return df.repartition(
        num_partitions
    )


# ============================================================
# REPARTITION BY COLUMN
# ============================================================

def repartition_by_column(
    df: DataFrame,
    num_partitions: int,
    column: str
) -> DataFrame:
    """
    Redistribute data based on a column.

    Records having the same column value
    are directed toward the same partition.
    """

    if num_partitions <= 0:
        raise ValueError(
            "Number of partitions must be greater than 0."
        )

    if column not in df.columns:
        raise ValueError(
            f"Column '{column}' does not exist."
        )

    return df.repartition(
        num_partitions,
        column
    )


# ============================================================
# COALESCE
# ============================================================

def coalesce_data(
    df: DataFrame,
    num_partitions: int
) -> DataFrame:
    """
    Reduce the number of partitions without
    performing a full shuffle.
    """

    if num_partitions <= 0:
        raise ValueError(
            "Number of partitions must be greater than 0."
        )

    current_partitions = get_partition_count(df)

    if num_partitions > current_partitions:
        raise ValueError(
            "coalesce() should normally be used "
            "to reduce the number of partitions."
        )

    return df.coalesce(
        num_partitions
    )