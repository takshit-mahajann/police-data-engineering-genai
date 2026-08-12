import json
from pathlib import Path
from datetime import datetime

from api_client import fetch_data


# ============================================================
# CONFIGURATION
# ============================================================

CHECKPOINT_FILE = Path("checkpoint.json")

RAW_DIR = Path("raw")

BATCH_SIZE = 100


# ============================================================
# CHECKPOINT FUNCTIONS
# ============================================================

def read_checkpoint():
    """
    Read the last successfully processed timestamp.
    """

    if not CHECKPOINT_FILE.exists():
        return None

    with open(
        CHECKPOINT_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        checkpoint = json.load(file)

    return checkpoint.get("last_updated")


def save_checkpoint(timestamp):
    """
    Save the latest successfully processed timestamp.
    """

    checkpoint = {
        "last_updated": timestamp
    }

    with open(
        CHECKPOINT_FILE,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            checkpoint,
            file,
            indent=2
        )

    print(
        f"Checkpoint updated: {timestamp}"
    )


# ============================================================
# QUERY BUILDING
# ============================================================

def build_query(last_updated):
    """
    Build the API query.

    First run:
        Fetch the oldest records.

    Later runs:
        Fetch records after the previous checkpoint.
    """

    if last_updated is None:

        return f"""
        SELECT *
        ORDER BY last_updated ASC
        LIMIT {BATCH_SIZE}
        """

    return f"""
    SELECT *
    WHERE last_updated > '{last_updated}'
    ORDER BY last_updated ASC
    LIMIT {BATCH_SIZE}
    """


# ============================================================
# DATA VALIDATION
# ============================================================

def validate_data(data):
    """
    Perform basic data-quality checks.

    Required fields must exist.

    Description is treated as optional because
    the API may omit it in some batches.
    """

    if not data:
        return False

    # --------------------------------------------------------
    # Required columns
    # --------------------------------------------------------

    required_columns = {
        "id",
        "date_time",
        "type",
        "subtype",
        "location",
        "last_updated"
    }

    actual_columns = set(data[0].keys())

    missing_columns = (
        required_columns - actual_columns
    )

    if missing_columns:

        print(
            "Missing required columns:",
            missing_columns
        )

        print("\nFirst record received:")

        print(
            json.dumps(
                data[0],
                indent=2
            )
        )

        print("\nColumns actually received:")

        print(
            data[0].keys()
        )

        raise ValueError(
            f"Missing required columns: "
            f"{missing_columns}"
        )

    # --------------------------------------------------------
    # Optional description column
    # --------------------------------------------------------

    if "description" not in actual_columns:

        print(
            "WARNING: 'description' column "
            "is not present in this batch."
        )

        print(
            "It will be handled as NULL "
            "during transformation."
        )

    # --------------------------------------------------------
    # Duplicate ID check
    # --------------------------------------------------------

    ids = [
        record.get("id")
        for record in data
        if record.get("id") is not None
    ]

    if len(ids) != len(set(ids)):

        print(
            "WARNING: Duplicate IDs found "
            "inside this batch."
        )

    return True


# ============================================================
# RAW DATA STORAGE
# ============================================================

def save_raw_batch(data, batch_number):
    """
    Save each ingestion batch separately.

    Data is partitioned by ingestion date so
    previous batches are not overwritten.
    """

    today = datetime.now().strftime(
        "%Y-%m-%d"
    )

    batch_dir = RAW_DIR / today

    batch_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    file_name = (
        f"batch_{batch_number}_{timestamp}.json"
    )

    file_path = batch_dir / file_name

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            indent=2
        )

    print(
        f"Saved batch {batch_number}: "
        f"{file_path}"
    )

    return file_path


# ============================================================
# FIND WATERMARK
# ============================================================

def get_max_timestamp(data):
    """
    Find the newest last_updated timestamp
    from the successfully processed batch.
    """

    timestamps = [
        record.get("last_updated")
        for record in data
        if record.get("last_updated")
    ]

    if not timestamps:

        raise ValueError(
            "No valid last_updated values "
            "found in the batch."
        )

    return max(timestamps)


# ============================================================
# MAIN INGESTION
# ============================================================

def main():

    print("\n")

    print("=" * 60)

    print(
        "CAMBRIDGE POLICE DATA INGESTION"
    )

    print("=" * 60)

    # --------------------------------------------------------
    # 1. Read previous checkpoint
    # --------------------------------------------------------

    last_updated = read_checkpoint()

    print(
        f"Previous checkpoint: "
        f"{last_updated}"
    )

    batch_number = 1

    total_records = 0

    # --------------------------------------------------------
    # 2. Keep fetching batches
    # --------------------------------------------------------

    while True:

        print("\n" + "-" * 60)

        print(
            f"Fetching batch {batch_number}..."
        )

        # ----------------------------------------------------
        # 3. Build query
        # ----------------------------------------------------

        query = build_query(
            last_updated
        )

        print("\nQuery:")

        print(query)

        # ----------------------------------------------------
        # 4. Call API
        # ----------------------------------------------------

        print(
            "Fetching data..."
        )

        data = fetch_data(query)

        record_count = len(data)

        print(
            f"Records received: "
            f"{record_count}"
        )

        # ----------------------------------------------------
        # 5. No more records
        # ----------------------------------------------------

        if not data:

            print(
                "\nNo more records available."
            )

            break

        # ----------------------------------------------------
        # 6. Validate batch
        # ----------------------------------------------------

        print(
            "Validating batch..."
        )

        validate_data(data)

        # ----------------------------------------------------
        # 7. Save raw batch
        # ----------------------------------------------------

        print(
            "Saving batch..."
        )

        save_raw_batch(
            data,
            batch_number
        )

        # ----------------------------------------------------
        # 8. Find new checkpoint
        # ----------------------------------------------------

        new_checkpoint = (
            get_max_timestamp(data)
        )

        # ----------------------------------------------------
        # 9. Update checkpoint
        # ----------------------------------------------------
        #
        # IMPORTANT:
        # Checkpoint is updated only AFTER
        # the batch has been successfully saved.
        #

        save_checkpoint(
            new_checkpoint
        )

        # ----------------------------------------------------
        # 10. Update counters/state
        # ----------------------------------------------------

        total_records += record_count

        last_updated = new_checkpoint

        print(
            f"Batch {batch_number} "
            f"completed."
        )

        print(
            f"Batch records: "
            f"{record_count}"
        )

        print(
            f"Total records this run: "
            f"{total_records}"
        )

        print(
            f"Current checkpoint: "
            f"{last_updated}"
        )

        # ----------------------------------------------------
        # 11. Move to next batch
        # ----------------------------------------------------

        batch_number += 1

    # ========================================================
    # FINAL REPORT
    # ========================================================

    print("\n")

    print("=" * 60)

    print(
        "INGESTION SUMMARY"
    )

    print("=" * 60)

    print(
        f"Total records ingested: "
        f"{total_records}"
    )

    print(
        f"Total batches: "
        f"{batch_number - 1}"
    )

    print(
        f"Final checkpoint: "
        f"{last_updated}"
    )

    print(
        "Ingestion completed successfully."
    )


# ============================================================
# PROGRAM ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()