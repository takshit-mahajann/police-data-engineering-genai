from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek
)


# ============================================================
# CLEAN DATA
# ============================================================

def clean_data(df: DataFrame) -> DataFrame:
    """
    Clean Cambridge Police DataFrame.

    Cleaning steps:
    1. Remove API metadata columns
    2. Clean string columns
    3. Standardize type and location
    4. Handle missing description
    5. Remove records with missing critical fields
    6. Remove duplicate records
    7. Create time dimension columns
    """

    print("\nStarting data cleaning...")


    # ========================================================
    # 1. REMOVE API METADATA
    # ========================================================

    metadata_columns = [
        ":id",
        ":version",
        ":created_at",
        ":updated_at"
    ]

    existing_metadata_columns = [
        column
        for column in metadata_columns
        if column in df.columns
    ]

    if existing_metadata_columns:

        print(
            "Removing metadata columns:",
            existing_metadata_columns
        )

        df = df.drop(
            *existing_metadata_columns
        )


    # ========================================================
    # 2. CLEAN STRING COLUMNS
    # ========================================================

    string_columns = [
        "id",
        "type",
        "subtype",
        "location",
        "description"
    ]

    for column_name in string_columns:

        if column_name in df.columns:

            df = df.withColumn(
                column_name,
                trim(col(column_name))
            )


    # ========================================================
    # 3. STANDARDIZE TYPE
    # ========================================================

    if "type" in df.columns:

        df = df.withColumn(
            "type",
            upper(col("type"))
        )


    # ========================================================
    # 4. STANDARDIZE LOCATION
    # ========================================================

    if "location" in df.columns:

        df = df.withColumn(
            "location",
            upper(col("location"))
        )


    # ========================================================
    # 5. HANDLE MISSING DESCRIPTION
    # ========================================================

    if "description" in df.columns:

        df = df.withColumn(
            "description",
            when(
                col("description").isNull()
                | (trim(col("description")) == ""),
                "No description available"
            ).otherwise(
                col("description")
            )
        )


    # ========================================================
    # 6. REMOVE RECORDS WITH MISSING CRITICAL FIELDS
    # ========================================================

    critical_columns = [
        "id",
        "date_time",
        "type",
        "last_updated"
    ]

    existing_critical_columns = [
        column
        for column in critical_columns
        if column in df.columns
    ]

    before_required_filter = df.count()

    df = df.dropna(
        subset=existing_critical_columns
    )

    after_required_filter = df.count()

    print(
        "Records removed because of "
        f"missing critical fields: "
        f"{before_required_filter - after_required_filter}"
    )


    # ========================================================
    # 7. REMOVE DUPLICATES
    # ========================================================

    before_deduplication = df.count()

    if "id" in df.columns:

        df = df.dropDuplicates(
            ["id"]
        )

    else:

        df = df.dropDuplicates()

    after_deduplication = df.count()

    print(
        "Duplicate records removed:",
        before_deduplication - after_deduplication
    )


    # ========================================================
    # 8. CREATE TIME DIMENSION COLUMNS
    # ========================================================

    print(
        "Creating time dimension columns..."
    )

    df = (
        df
        .withColumn(
            "year",
            year(col("date_time"))
        )
        .withColumn(
            "month",
            month(col("date_time"))
        )
        .withColumn(
            "day",
            dayofmonth(col("date_time"))
        )
        .withColumn(
            "hour",
            hour(col("date_time"))
        )
        .withColumn(
            "day_of_week",
            dayofweek(col("date_time"))
        )
    )


    # ========================================================
    # FINAL COUNT
    # ========================================================

    final_count = df.count()

    print(
        "Records after cleaning:",
        final_count
    )

    print(
        "Data cleaning completed."
    )

    return df