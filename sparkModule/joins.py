from pyspark.sql import DataFrame


# ============================================================
# INNER JOIN
# ============================================================

def inner_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: list[str]
) -> DataFrame:
    """
    Return only records that have matching keys
    in both DataFrames.
    """

    return left_df.join(
        right_df,
        on=join_columns,
        how="inner"
    )


# ============================================================
# LEFT JOIN
# ============================================================

def left_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: list[str]
) -> DataFrame:
    """
    Keep all records from the left DataFrame.

    Matching records from the right DataFrame
    are added where available.
    """

    return left_df.join(
        right_df,
        on=join_columns,
        how="left"
    )


# ============================================================
# RIGHT JOIN
# ============================================================

def right_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: list[str]
) -> DataFrame:
    """
    Keep all records from the right DataFrame.

    Matching records from the left DataFrame
    are added where available.
    """

    return left_df.join(
        right_df,
        on=join_columns,
        how="right"
    )


# ============================================================
# FULL OUTER JOIN
# ============================================================

def full_outer_join(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: list[str]
) -> DataFrame:
    """
    Keep all records from both DataFrames.
    """

    return left_df.join(
        right_df,
        on=join_columns,
        how="full"
    )