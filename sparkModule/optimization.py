from pyspark.sql import DataFrame


# ============================================================
# EXPLAIN EXECUTION PLAN
# ============================================================

def explain_plan(
    df: DataFrame,
    extended: bool = True
) -> None:
    """
    Display Spark's execution plan.
    """

    df.explain(
        extended=extended
    )


# ============================================================
# GET PARTITION COUNT
# ============================================================

def get_partition_count(
    df: DataFrame
) -> int:
    """
    Return the current number of partitions.
    """

    return df.rdd.getNumPartitions()


# ============================================================
# BROADCAST DATAFRAME
# ============================================================

def broadcast_dataframe(
    df: DataFrame
) -> DataFrame:
    """
    Mark a DataFrame for broadcast joins.

    This is useful when the DataFrame is small enough
    to be distributed to Spark executors.
    """

    from pyspark.sql.functions import broadcast

    return broadcast(df)