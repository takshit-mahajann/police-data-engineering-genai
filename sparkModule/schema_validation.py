from pyspark.sql import DataFrame

from logger import get_logger


logger = get_logger()


# ============================================================
# REQUIRED COLUMNS
# ============================================================

REQUIRED_COLUMNS = [
    "id",
    "date_time",
    "type",
    "location"
]


# ============================================================
# VALIDATE REQUIRED COLUMNS
# ============================================================

def validate_required_columns(
    df: DataFrame
) -> bool:
    """
    Check whether all required columns
    are present in the DataFrame.
    """

    actual_columns = set(
        df.columns
    )

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in actual_columns
    ]

    if missing_columns:

        print(
            "\nSchema validation failed."
        )

        print(
            f"Missing columns: "
            f"{missing_columns}"
        )

        logger.error(
            f"Missing required columns: "
            f"{missing_columns}"
        )

        return False

    logger.info(
        "Required columns are present."
    )

    return True


# ============================================================
# VALIDATE DATA TYPES
# ============================================================

def validate_data_types(
    df: DataFrame
) -> bool:
    """
    Check important column data types.
    """

    expected_types = {
        "id": "string",
        "date_time": "timestamp",
        "type": "string",
        "location": "string"
    }

    actual_types = dict(
        df.dtypes
    )

    errors = []

    for column, expected_type in expected_types.items():

        actual_type = actual_types.get(
            column
        )

        if actual_type != expected_type:

            errors.append(
                f"{column}: expected "
                f"{expected_type}, got "
                f"{actual_type}"
            )

    if errors:

        print(
            "\nData type validation failed:"
        )

        for error in errors:

            print(
                f"  - {error}"
            )

        logger.error(
            "Data type validation failed."
        )

        return False

    logger.info(
        "Data types are valid."
    )

    return True


# ============================================================
# COMPLETE SCHEMA VALIDATION
# ============================================================

def validate_schema(
    df: DataFrame
) -> bool:
    """
    Run all schema validation checks.
    """

    print(
        "\nRunning schema validation..."
    )

    columns_valid = (
        validate_required_columns(df)
    )

    if not columns_valid:
        return False

    types_valid = (
        validate_data_types(df)
    )

    if not types_valid:
        return False

    print(
        "Schema validation passed."
    )

    logger.info(
        "Schema validation passed."
    )

    return True