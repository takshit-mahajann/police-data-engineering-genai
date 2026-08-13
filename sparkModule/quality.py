from pyspark.sql import DataFrame
from pyspark.sql.functions import col


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

def run_quality_checks(df: DataFrame) -> bool:
    """
    Run data-quality checks on the cleaned DataFrame.

    Returns:
        True  -> all critical checks passed
        False -> one or more checks failed
    """

    print("\n")
    print("=" * 60)
    print("DATA QUALITY CHECKS")
    print("=" * 60)

    checks_passed = True

    # ========================================================
    # 1. RECORD COUNT
    # ========================================================

    record_count = df.count()

    print(
        f"\nTotal records: {record_count}"
    )

    if record_count == 0:

        print(
            "❌ FAILED: DataFrame contains no records."
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: DataFrame contains records."
        )

    # ========================================================
    # 2. REQUIRED COLUMNS
    # ========================================================

    required_columns = {
        "id",
        "date_time",
        "type",
        "last_updated"
    }

    actual_columns = set(df.columns)

    missing_columns = (
        required_columns - actual_columns
    )

    if missing_columns:

        print(
            f"❌ FAILED: Missing columns: "
            f"{missing_columns}"
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: Required columns exist."
        )

    # ========================================================
    # 3. NULL ID CHECK
    # ========================================================

    null_ids = df.filter(
        col("id").isNull()
    ).count()

    print(
        f"\nNull IDs: {null_ids}"
    )

    if null_ids > 0:

        print(
            "❌ FAILED: Null IDs found."
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: No null IDs."
        )

    # ========================================================
    # 4. NULL DATE CHECK
    # ========================================================

    null_dates = df.filter(
        col("date_time").isNull()
    ).count()

    print(
        f"\nNull date_time values: "
        f"{null_dates}"
    )

    if null_dates > 0:

        print(
            "❌ FAILED: Null date_time values found."
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: No null date_time values."
        )

    # ========================================================
    # 5. NULL LAST_UPDATED CHECK
    # ========================================================

    null_last_updated = df.filter(
        col("last_updated").isNull()
    ).count()

    print(
        f"\nNull last_updated values: "
        f"{null_last_updated}"
    )

    if null_last_updated > 0:

        print(
            "❌ FAILED: Null last_updated values found."
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: No null last_updated values."
        )

    # ========================================================
    # 6. DUPLICATE ID CHECK
    # ========================================================

    duplicate_ids = (
        df.groupBy("id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(
        f"\nDuplicate IDs: {duplicate_ids}"
    )

    if duplicate_ids > 0:

        print(
            "❌ FAILED: Duplicate IDs found."
        )

        checks_passed = False

    else:

        print(
            "✅ PASSED: No duplicate IDs."
        )

    # ========================================================
    # FINAL RESULT
    # ========================================================

    print("\n" + "-" * 60)

    if checks_passed:

        print(
            "✅ ALL DATA QUALITY CHECKS PASSED"
        )

    else:

        print(
            "❌ DATA QUALITY CHECKS FAILED"
        )

    print("-" * 60)

    return checks_passed