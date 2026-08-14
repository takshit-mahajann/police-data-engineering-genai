from pyspark.sql import DataFrame
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    lag,
    lead,
    row_number,
    rank,
    dense_rank,
    unix_timestamp,
    count,
    col
)


# ============================================================
# 1. PREVIOUS INCIDENT
# ============================================================

def add_previous_incident(df: DataFrame) -> DataFrame:
    """
    Add the previous incident timestamp
    for each location.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy("date_time")
    )

    return df.withColumn(
        "previous_date_time",
        lag("date_time").over(window_spec)
    )


# ============================================================
# 2. NEXT INCIDENT
# ============================================================

def add_next_incident(df: DataFrame) -> DataFrame:
    """
    Add the next incident timestamp
    for each location.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy("date_time")
    )

    return df.withColumn(
        "next_date_time",
        lead("date_time").over(window_spec)
    )


# ============================================================
# 3. ROW NUMBER
# ============================================================

def add_row_number(df: DataFrame) -> DataFrame:
    """
    Assign a sequential row number to incidents
    within each location.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy("date_time")
    )

    return df.withColumn(
        "incident_row_number",
        row_number().over(window_spec)
    )


# ============================================================
# 4. RANK
# ============================================================

def add_rank(df: DataFrame) -> DataFrame:
    """
    Rank incidents within each location
    based on date_time.

    Ties receive the same rank and can create gaps.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy("date_time")
    )

    return df.withColumn(
        "incident_rank",
        rank().over(window_spec)
    )


# ============================================================
# 5. DENSE RANK
# ============================================================

def add_dense_rank(df: DataFrame) -> DataFrame:
    """
    Rank incidents within each location
    without gaps between ranks.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy("date_time")
    )

    return df.withColumn(
        "incident_dense_rank",
        dense_rank().over(window_spec)
    )


# ============================================================
# 6. TIME SINCE PREVIOUS INCIDENT
# ============================================================

def add_minutes_since_previous(df: DataFrame) -> DataFrame:
    """
    Calculate the number of minutes between
    the current incident and the previous incident
    at the same location.
    """

    df = add_previous_incident(df)

    df = df.withColumn(
        "minutes_since_previous",
        (
            unix_timestamp("date_time")
            - unix_timestamp("previous_date_time")
        ) / 60
    )

    return df


# ============================================================
# 7. ROLLING INCIDENT COUNT
# ============================================================

def add_rolling_incident_count(
    df: DataFrame,
    days: int = 7
) -> DataFrame:
    """
    Calculate the number of incidents at each location
    within the previous N days, including the current incident.

    Example:
        days=7

    Counts incidents occurring within the
    previous 7 * 24 hours.
    """

    window_spec = (
        Window
        .partitionBy("location")
        .orderBy(
            col("date_time").cast("timestamp").cast("long")
        )
        .rangeBetween(
            -days * 24 * 60 * 60,
            0
        )
    )

    return df.withColumn(
        f"rolling_{days}_day_incident_count",
        count("*").over(window_spec)
    )