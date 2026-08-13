from pathlib import Path

from pyspark.sql import DataFrame


# ============================================================
# WRITE CURATED DATA
# ============================================================

def write_curated_data(
    df: DataFrame,
    output_path: Path
):
    """
    Write cleaned police data as partitioned Parquet.

    Data is physically partitioned by:
        year
        month
    """

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print("\nWriting curated data...")
    print("Partitioning by: year, month")

    (
        df.write
        .mode("overwrite")
        .partitionBy(
            "year",
            "month"
        )
        .parquet(
            str(output_path)
        )
    )

    print(
        f"Curated data written to: "
        f"{output_path}"
    )


# ============================================================
# WRITE ANALYTICS DATA
# ============================================================

def write_analytics_data(
    df: DataFrame,
    output_path: Path
):
    """
    Write analytics DataFrame as Parquet.
    """

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"\nWriting analytics data: "
        f"{output_path}"
    )

    (
        df.write
        .mode("overwrite")
        .parquet(
            str(output_path)
        )
    )

    print(
        f"Analytics data written to: "
        f"{output_path}"
    )