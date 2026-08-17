from pathlib import Path

from pyspark.sql import DataFrame


# ============================================================
# WRITE CURATED DATA
# ============================================================

def write_curated_data(
    df: DataFrame,
    output_path: Path,
    mode: str = "overwrite"
):
    """
    Write curated police data as partitioned Parquet.

    Data is physically partitioned by:
        year
        month

    Parameters
    ----------
    df : DataFrame
        Curated Spark DataFrame.

    output_path : Path
        Curated data output directory.

    mode : str
        Spark write mode.

        overwrite:
            Replace existing curated data.

        append:
            Add new records to existing curated data.
    """

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print("\nWriting curated data...")
    print("Partitioning by: year, month")
    print(f"Write mode: {mode}")

    (
        df.write
        .mode(mode)
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
    output_path: Path,
    mode: str = "overwrite"
):
    """
    Write analytics DataFrame as Parquet.

    Analytics data is overwritten by default because
    aggregations are recalculated from the complete
    curated dataset.
    """

    output_path.mkdir(
        parents=True,
        exist_ok=True
    )

    print(
        f"\nWriting analytics data: "
        f"{output_path}"
    )

    print(
        f"Write mode: {mode}"
    )

    (
        df.write
        .mode(mode)
        .parquet(
            str(output_path)
        )
    )

    print(
        f"Analytics data written to: "
        f"{output_path}"
    )