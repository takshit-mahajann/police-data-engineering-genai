from pyspark.sql import DataFrame


# ============================================================
# REMOVE DUPLICATES WITHIN CURRENT BATCH
# ============================================================

def remove_batch_duplicates(
    df: DataFrame
) -> DataFrame:
    """
    Remove duplicate incidents within the current batch.

    The incident ID is treated as the unique business key.
    """

    before_count = df.count()

    df = df.dropDuplicates(
        ["id"]
    )

    after_count = df.count()

    removed = before_count - after_count

    print(
        f"Batch duplicate records removed: {removed}"
    )

    return df


# ============================================================
# REMOVE RECORDS ALREADY IN CURATED DATA
# ============================================================

def remove_existing_records(
    df: DataFrame,
    curated_df: DataFrame
) -> DataFrame:
    """
    Remove incidents from the new batch that already exist
    in the curated dataset.

    Uses the incident 'id' as the unique key.
    """

    before_count = df.count()

    existing_ids = (
        curated_df
        .select("id")
        .where("id IS NOT NULL")
        .dropDuplicates(["id"])
    )

    df = df.join(
        existing_ids,
        on="id",
        how="left_anti"
    )

    after_count = df.count()

    removed = before_count - after_count

    print(
        f"Existing records removed: {removed}"
    )

    return df