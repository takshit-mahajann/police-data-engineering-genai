from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    count,
    year,
    month,
    hour,
    desc
)


# ============================================================
# INCIDENTS BY TYPE
# ============================================================

def incidents_by_type(df: DataFrame) -> DataFrame:
    """
    Count police records by type.
    """

    return (
        df
        .groupBy("type")
        .agg(
            count("*").alias("total_records")
        )
        .orderBy(
            desc("total_records")
        )
    )


# ============================================================
# INCIDENTS BY MONTH
# ============================================================

def incidents_by_month(df: DataFrame) -> DataFrame:
    """
    Count police records by year and month.
    """

    return (
        df
        .groupBy(
            year("date_time").alias("year"),
            month("date_time").alias("month")
        )
        .agg(
            count("*").alias("total_records")
        )
        .orderBy(
            "year",
            "month"
        )
    )


# ============================================================
# INCIDENTS BY LOCATION
# ============================================================

def incidents_by_location(df: DataFrame) -> DataFrame:
    """
    Count police records by location.
    """

    return (
        df
        .groupBy("location")
        .agg(
            count("*").alias("total_records")
        )
        .orderBy(
            desc("total_records")
        )
    )


# ============================================================
# INCIDENTS BY HOUR
# ============================================================

def incidents_by_hour(df: DataFrame) -> DataFrame:
    """
    Count police records by hour of day.
    """

    return (
        df
        .groupBy(
            hour("date_time").alias("hour")
        )
        .agg(
            count("*").alias("total_records")
        )
        .orderBy("hour")
    )


# ============================================================
# INCIDENTS BY SUBTYPE
# ============================================================

def incidents_by_subtype(df: DataFrame) -> DataFrame:
    """
    Count police records by subtype.
    """

    return (
        df
        .groupBy("subtype")
        .agg(
            count("*").alias("total_records")
        )
        .orderBy(
            desc("total_records")
        )
    )